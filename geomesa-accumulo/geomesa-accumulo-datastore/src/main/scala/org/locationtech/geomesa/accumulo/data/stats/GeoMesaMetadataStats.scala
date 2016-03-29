/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.stats

import java.nio.charset.Charset
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.mock.MockConnector
import org.geotools.data.{Query, Transaction}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time._
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator
import org.locationtech.geomesa.accumulo.util.{DistributedLocking, SelfClosingIterator}
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, SimpleFeatureTypes, wholeWorldEnvelope}
import org.locationtech.geomesa.utils.stats._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object GeoMesaMetadataStats {

  val dtFormat = ISODateTimeFormat.dateTime().withZoneUTC()

  val DefaultUpdateInterval = 360  // in minutes
  val GeometryHistogramSize = 1024 // corresponds to 2 digits of geohash - 1,252.3km lon 624.1km lat
  val MinGeom = WKTUtils.read("POINT (-180 -90)")
  val MaxGeom = WKTUtils.read("POINT (180 90)")

  private [stats] val Utf8 = Charset.forName("UTF-8")

  def allTimeBounds = new Interval(0L, System.currentTimeMillis(), DateTimeZone.UTC) // epoch till now

  /**
    * Gets a safe string to use as a key in accumulo
    *
    * @param input input string
    * @return safe encoded string, all alphanumeric or underscore
    */
  def keySafeString(input: String): String = GeoMesaTable.hexEncodeNonAlphaNumeric(input)

  private [stats] def lockKey(table: String, typeName: String) = {
    val safeName = GeoMesaTable.hexEncodeNonAlphaNumeric(typeName)
    s"/org.locationtech.geomesa/accumulo/stats/$table/$safeName"
  }

  private [stats] def minMaxKey(attribute: String): String =
    s"$STATS_BOUNDS_PREFIX-${keySafeString(attribute)}"

  private [stats] def histogramKey(attribute: String): String =
    s"$STATS_HISTOGRAM_PREFIX-${keySafeString(attribute)}"

  private [stats] def okForMinMax(d: AttributeDescriptor): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    // TODO support list/map types in stats
    d.isIndexed && !d.isMultiValued && d.getType.getBinding != classOf[java.lang.Boolean]
  }
}

/**
 * Tracks stats via entries stored in metadata. Implements runnable to update stats. Normally
 * this class is self-scheduling, but can be manually run to update stats immediately.
 *
 * Before updating stats, acquires a distributed lock to ensure that we aren't
 * duplicating effort.
 */
class GeoMesaMetadataStats(ds: AccumuloDataStore, initialDelayMinutes: Int = 10) extends
    GeoMesaStats with DistributedLocking with Runnable with LazyLogging {

  import GeoMesaMetadataStats._

  val connector = ds.connector

  private val es = Executors.newSingleThreadScheduledExecutor()
  private val shutdown = new AtomicBoolean(false)

  private val serializers = new SoftThreadLocalCache[String, StatSerializer]()

  // start the background thread to gather stats
  // we schedule the initial run for 10 minutes - this should avoid overload during startup,
  // and prevent short-lived data stores from spamming updates
  if (!connector.isInstanceOf[MockConnector]) {
    es.schedule(this, initialDelayMinutes, TimeUnit.MINUTES)
  }

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Long = {
    if (exact) {
      // TODO stat query doesn't entirely handle duplicates - only on a per-iterator basis
      // we could do a full scan, but is it worth it?
      executeStatsQuery(Stat.Count(), sft, filter) match {
        case s: CountStat => s.count
        case s =>
          logger.warn("Got unexpected Count result, falling back to normal full scan: " +
              s"${if (s == null) "null" else s.toJson()}")
          // fall back to full scan - return no properties to reduce data transferred
          val query = new Query(sft.getTypeName, filter, Array.empty[String])
          SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).length
      }
    } else if (filter == Filter.INCLUDE) {
      ds.metadata.read(sft.getTypeName, STATS_TOTAL_COUNT_KEY, cache = false).map(_.toLong).getOrElse(-1)
    } else {
      // TODO use the histograms to estimate counts
      -1
    }
  }

  override def getBounds(sft: SimpleFeatureType, filter: Filter, exact: Boolean): ReferencedEnvelope = {
    val geom = sft.getGeomField
    if (geom == null) {
      // geometry-less schema
      wholeWorldEnvelope
    } else {
      val (min, max) = getMinMax[Geometry](sft, geom, filter, exact)
      val env = min.getEnvelopeInternal
      env.expandToInclude(max.getEnvelopeInternal)
      new ReferencedEnvelope(env, CRS_EPSG_4326)
    }
  }

  override def getMinMax[T](sft: SimpleFeatureType, attribute: String, filter: Filter, exact: Boolean): (T, T) = {

    def defaultMinMax: (T, T) = {
      val defaults = Stat(sft, Stat.MinMax(attribute)).asInstanceOf[MinMax[T]].defaults
      (defaults.max, defaults.min) // defaults are set up so any comparison will replace them - swap min/max
    }

    if (exact) {
      executeStatsQuery(Stat.MinMax(attribute), sft, filter) match {
        case s: MinMax[T] if s.min != null => (s.min, s.max)
        case s =>
          logger.warn(s"Got back unexpected MinMax: ${if (s == null) "null" else s.toJson()}")
          defaultMinMax
      }
    } else {
      readStat[MinMax[T]](sft, GeoMesaMetadataStats.minMaxKey(attribute)) match {
        case Some(s) if s.min != null => (s.min, s.max)
        case _ => defaultMinMax
      }
    }
  }

  override def getHistogram[T](sft: SimpleFeatureType, attribute: String): Option[RangeHistogram[T]] =
    readStat[RangeHistogram[T]](sft, histogramKey(attribute))

  override def getStatUpdater(sft: SimpleFeatureType): StatUpdater = {
    val statString = buildStatsFor(sft, update = true)
    val tracker: () => Option[Stat] = () => if (statString.isEmpty) None else Some(Stat(sft, statString))
    new MetadataStatUpdater(this, sft, tracker)
  }

  override def close(): Unit = {
    shutdown.set(true)
    es.shutdown()
  }

  /**
   * Checks for the last time stats were run, and runs if needed.
   * Updates metadata accordingly.
   */
  override def run(): Unit = {
    val nextUpdates = ArrayBuffer.empty[DateTime]

    // convert to iterator so we check shutdown before each update
    ds.getTypeNames.iterator.filter(_ => !shutdown.get()).foreach { typeName =>
      // try to get an exclusive lock on the sft - if not, don't wait just move along
      lock(lockKey(ds.catalogTable, typeName), 1000).foreach { lock =>
        try {
          val lastUpdate = getLastUpdate(typeName)
          val updateInterval = getUpdateInterval(typeName)
          val nextUpdate = lastUpdate.plusMinutes(updateInterval)

          if (nextUpdate.isAfterNow) {
            nextUpdates.append(nextUpdate)
          } else {
            // run the update
            try {
              update(typeName)
              // update the metadata
              val updated = DateTime.now(DateTimeZone.UTC)
              setLastUpdate(typeName, updated)
              nextUpdates.append(updated.plusMinutes(updateInterval))
            } catch {
              case e: Exception =>
                logger.error(s"Error running stat update for type $typeName:", e)
                // after error, don't schedule again until next interval has passed
                // TODO track failures and don't just keep retrying...
                nextUpdates.append(nextUpdate.plusMinutes(updateInterval))
            }
          }
        } finally {
          lock.release()
        }
      }
    }

    if (!shutdown.get()) {
      implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

      val nextScheduled = Some(nextUpdates).collect { case u if u.nonEmpty => u.min } match {
        case Some(next) if next.isAfterNow => next.getMillis - DateTimeUtils.currentTimeMillis()
        case _ => 60000L // default to check again in 60s
      }
      es.schedule(this, nextScheduled, TimeUnit.MILLISECONDS)
    }
  }

  /**
    * Updates the stats for this sft.
    *
    * Note: this method assumes that we have an exclusive lock on the sft
    *
    * @param typeName simple feature type name
    */
  private def update(typeName: String): Unit = {
    val sft = ds.getSchema(typeName)
    val statString = buildStatsFor(sft)

    if (statString.isEmpty) {
      logger.debug(s"Not calculating any stats for sft $typeName: ${SimpleFeatureTypes.encodeType(sft)}")
    } else {
      logger.debug(s"Calculating stats for $typeName: $statString")

      val stats = executeStatsQuery(statString, sft)

      logger.trace(s"Stats for $typeName: ${stats.toJson()}")
      logger.debug(s"Writing stats for $typeName")

      writeStat(stats, sft)
    }
  }

  /**
    * Execute a query against accumulo to calculate stats
    *
    * @param stats stat string
    * @param sft simple feature type
    * @param filter ecql filter for feature selection
    * @return stats
    */
  private def executeStatsQuery(stats: String, sft: SimpleFeatureType, filter: Filter = Filter.INCLUDE): Stat = {
    val query = new Query(sft.getTypeName, filter)
    query.getHints.put(QueryHints.STATS_KEY, stats)
    query.getHints.put(QueryHints.RETURN_ENCODED_KEY, java.lang.Boolean.TRUE)

    val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    try {
      // stats should always return exactly one result, even if there are no features in the table
      KryoLazyStatsIterator.decodeStat(reader.next.getAttribute(0).asInstanceOf[String], sft)
    } finally {
      reader.close()
    }
  }

  /**
    * Write a stat to accumulo. If update == true, will attempt to merge the existing stat
    * and the new one, otherwise will overwrite.
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param merge merge with the existing stat - otherwise overwrite
    */
  private def writeStat(stat: Stat, sft: SimpleFeatureType, merge: Boolean = false): Unit = {
    stat match {
      case s: MinMax[_]         => writeMinMax(s, sft, merge)
      case s: RangeHistogram[_] => writeHistogram(s, sft, merge)
      case s: CountStat         => writeCount(s, sft, merge)
      case s: SeqStat           => s.stats.foreach(writeStat(_, sft, merge))
      case _ => throw new NotImplementedError("Only Count, MinMax and RangeHistogram stats are supported")
    }
  }

  /**
    * Acquires the lock before updating the stat
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param lockTimeout how long to wait for the lock, in milliseconds
    * @return true if stat was updated, else false
    */
  private [stats] def writeStatUpdate(stat: Stat, sft: SimpleFeatureType, lockTimeout: Long): Boolean = {
    lock(lockKey(ds.catalogTable, sft.getTypeName), lockTimeout) match {
      case Some(lock) => try { writeStat(stat, sft, merge = true) } finally { lock.release() }; true
      case None => false
    }
  }

  private def writeMinMax(stat: MinMax[_], sft: SimpleFeatureType, merge: Boolean = false): Unit = {
    val key = minMaxKey(sft.getDescriptor(stat.attribute).getLocalName)

    val existing = readStat[MinMax[_]](sft, key)
    val updated = if (merge) tryMerge(stat, existing) else stat

    if (!existing.contains(updated)) {
      val value = serializer(sft).serialize(updated)
      ds.metadata.insert(sft.getTypeName, key, new String(value, Utf8))
    }
  }

  private def writeHistogram(stat: RangeHistogram[_], sft: SimpleFeatureType, merge: Boolean = false): Unit = {
    val key = histogramKey(sft.getDescriptor(stat.attribute).getLocalName)

    val existing = readStat[RangeHistogram[Any]](sft, key)
    val updated = if (merge) tryMerge(stat, existing) else stat

    if (!existing.contains(updated)) {
      val value = serializer(sft).serialize(updated)
      ds.metadata.insert(sft.getTypeName, key, new String(value, Utf8))
    }
  }

  private def writeCount(stat: CountStat, sft: SimpleFeatureType, merge: Boolean = false): Unit = {
    val existing = ds.metadata.read(sft.getTypeName, STATS_TOTAL_COUNT_KEY, cache = false).map(_.toLong)
    val updated = if (merge) existing.map(_ + stat.count).getOrElse(stat.count) else stat.count
    if (!existing.contains(updated)) {
      ds.metadata.insert(sft.getTypeName, STATS_TOTAL_COUNT_KEY, updated.toString)
    }
  }

  private def tryMerge(stat: Stat, existing: Option[Stat]): Stat =
    existing.flatMap(e => Try(stat + e).toOption).getOrElse(stat)

  private def readStat[T <: Stat](sft: SimpleFeatureType, key: String): Option[T] =
    ds.metadata.read(sft.getTypeName, key, cache = false)
        .flatMap(s => Try(serializer(sft).deserialize(s.getBytes(Utf8))).toOption)
        .collect { case s: T if !s.isEmpty => s }

  private def buildStatsFor(sft: SimpleFeatureType, update: Boolean = false): String = {
    import GeoMesaMetadataStats.{GeometryHistogramSize, MaxGeom, MinGeom}


    val count = Stat.Count()
    val geomHistogram = Option(sft.getGeomField).map(g => Stat.RangeHistogram(g, GeometryHistogramSize, MinGeom, MaxGeom))

    val dateHistogram = for {
      dtg <- sft.getDtgField
      bounds <- readStat[MinMax[Date]](sft, minMaxKey(dtg))
      start = bounds.min
      // project one day into the future to account for new data being written
      end = if (update) DateTime.now(DateTimeZone.UTC).plusDays(1).toDate else bounds.max
      weeks = new Duration(start.getTime, end.getTime).getStandardDays / 7
      if weeks > 1
    } yield {
      Stat.RangeHistogram(dtg, weeks.toInt, start, end)
    }

    val minMax = {
      import scala.collection.JavaConversions._
      val indexed = sft.getAttributeDescriptors.filter(okForMinMax).map(_.getLocalName)
      (Option(sft.getGeomField).toSeq ++ sft.getDtgField ++ indexed).distinct.map(Stat.MinMax)
    }

    Stat.SeqStat(Seq(count) ++ geomHistogram ++ dateHistogram ++ minMax)
  }

  /**
    * Reads the time of the last update
    *
    * @param typeName simple feature type name
    * @return time of the last update
    */
  private def getLastUpdate(typeName: String): DateTime = {
    ds.metadata.read(typeName, STATS_GENERATION_KEY, cache = false) match {
      case Some(dt) => dtFormat.parseDateTime(dt)
      case None     => new DateTime(0, DateTimeZone.UTC)
    }
  }

  /**
    * Persists the time of the last update
    *
    * @param typeName simple feature type name
    */
  private def setLastUpdate(typeName: String, update: DateTime = DateTime.now(DateTimeZone.UTC)): Unit =
    ds.metadata.insert(typeName, STATS_GENERATION_KEY, dtFormat.print(update))

  /**
    * Reads the update interval.
    *
    * Note: will write the default update interval if the data doesn't exist
    *
    * @param typeName simple feature type name
    * @return update interval, in minutes
    */
  private def getUpdateInterval(typeName: String): Int = {
    import GeoMesaMetadataStats.DefaultUpdateInterval

    ds.metadata.read(typeName, STATS_INTERVAL_KEY, cache = false) match {
      case Some(dt) => dt.toInt
      case None =>
        // write the default so that it's there if anyone wants to modify it
        ds.metadata.insert(typeName, STATS_INTERVAL_KEY, DefaultUpdateInterval.toString)
        DefaultUpdateInterval
    }
  }

  private def serializer(sft: SimpleFeatureType): StatSerializer =
    serializers.getOrElseUpdate(sft.getTypeName, StatSerializer(sft))
}

/**
  * Stores stats as metadata entries
  *
  * @param stats persistence
  * @param sft simple feature type
  * @param tracker creates stats for tracking new features
  */
class MetadataStatUpdater(stats: GeoMesaMetadataStats, sft: SimpleFeatureType, tracker: () => Option[Stat])
    extends StatUpdater with LazyLogging {

  private var currentTracker: Option[Stat] = null
  private var updateStats: (SimpleFeature) => Unit = null

  reloadTracker()

  private def reloadTracker(): Unit = {
    currentTracker = tracker()
    updateStats = currentTracker match {
      case Some(stat) => (f) => stat.observe(f)
      case None       => (_) => Unit
    }
  }

  override def update(f: SimpleFeature): Unit = updateStats(f)

  override def close(): Unit = if (!write(1000, reload = false)) {
    logger.warn(s"Failed to write update stats for schema ${sft.getTypeName}")
  }

  // don't wait around if the lock isn't available...
  override def flush(): Unit = write(10, reload = true)

  private def write(delay: Long, reload: Boolean): Boolean = {
    currentTracker match {
      case None => true
      case Some(t) =>
        val updated = stats.writeStatUpdate(t, sft, delay)
        if (updated && reload) {
          // reload the tracker
          // for long-held updaters, this will refresh the date histogram range
          reloadTracker()
        }
        updated
    }
  }
}