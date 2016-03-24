/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.stats

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.apache.accumulo.core.client.impl.{MasterClient, Tables}
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.security.thrift.TCredentials
import org.apache.accumulo.trace.instrument.Tracer
import org.geotools.data.{DataUtilities, Query, Transaction}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeUtils, DateTimeZone, Interval}
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.util.{DistributedLocking, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, wholeWorldEnvelope}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

/**
 * Tracks stats for a schema - spatial/temporal bounds, number of records, etc. Persistence of
 * stats is not part of this trait, as different implementations will likely have different method signatures.
 */
trait GeoMesaStats extends Closeable {

  /**
   * Gets the number of features that will be returned for a query
   *
   * @param typeName simple feature type name
   * @param filter cql filter
   * @param exact rough estimate, or precise count. note: precise count will likely be very expensive.
   * @return count of features
   */
  def getCount(typeName: String, filter: Filter = Filter.INCLUDE, exact: Boolean = false): Long

  /**
   * Gets the bounds for data that will be returned for a query
   *
   * @param typeName simple feature type name
   * @param filter cql filter
   * @param exact rough estimate, or precise bounds. note: precise bounds will likely be very expensive.
   * @return bounds
   */
  def getBounds(typeName: String, filter: Filter = Filter.INCLUDE, exact: Boolean = false): ReferencedEnvelope

  /**
   * Gets the temporal bounds for data that will be returned for a query
   *
   * @param typeName simple feature type name
   * @param filter cql filter
   * @param exact rough estimate, or precise bounds. note: precise bounds will likely be very expensive.
   * @return time bounds
   */
  def getTemporalBounds(typeName: String, filter: Filter = Filter.INCLUDE, exact: Boolean = false): Interval
}

trait HasGeoMesaStats {
  def stats: GeoMesaStats
}

/**
 * Tracks stats via entries stored in metadata. Implements runnable to update stats. Normally
 * this class is self-scheduling, but can be manually run to update stats immediately.
 *
 * Before updating stats, acquires a distributed lock to ensure that we aren't
 * duplicating effort.
 */
class GeoMesaMetadataStats(ds: AccumuloDataStore, initialDelayMinutes: Int = 5) extends
    GeoMesaStats with DistributedLocking with Runnable with LazyLogging {

  import GeoMesaStats._

  val connector = ds.connector

  // start the background thread to gather stats
  // we schedule the initial run for 5 minutes - this should avoid overload during startup,
  // and prevent short-lived data stores from spamming updates
  private val es = Executors.newSingleThreadScheduledExecutor()
  private val shutdown = new AtomicBoolean(false)
  if (!connector.isInstanceOf[MockConnector]) {
    es.schedule(this, initialDelayMinutes, TimeUnit.MINUTES)
  }

  // Note: we don't currently filter by the cql
  override def getBounds(typeName: String, filter: Filter, exact: Boolean): ReferencedEnvelope =
    readSpatialBounds(typeName)
        .map(new ReferencedEnvelope(_, CRS_EPSG_4326))
        .getOrElse(wholeWorldEnvelope)

  // Note: we don't currently filter by the cql
  override def getTemporalBounds(typeName: String, filter: Filter, exact: Boolean): Interval =
    readTemporalBounds(typeName).getOrElse(allTimeBounds)

  // Note: we don't currently filter by the cql
  override def getCount(typeName: String, filter: Filter, exact: Boolean): Long = {
    if (exact) {
      SelfClosingIterator(ds.getFeatureReader(new Query(typeName, filter), Transaction.AUTO_COMMIT)).length
    } else {
      -1L
    }
    // TODO
    // ad.getCardinality()
//    val attrsAndCounts = filter.primary
//        .flatMap(getAttributeProperty)
//        .map(_.name)
//        .groupBy((f: String) => f)
//        .map { case (name, itr) => (name, itr.size) }
//
//    val cost = attrsAndCounts.map { case (attr, count) =>
//      val descriptor = sft.getDescriptor(attr)
//      // join queries are much more expensive than non-join queries
//      // TODO we could consider whether a join is actually required based on the filter and transform
//      // TODO figure out the actual cost of each additional range...I'll make it 2
//      val additionalRangeCost = 1
//      val joinCost = 10
//      val multiplier = if (descriptor.getIndexCoverage() == IndexCoverage.JOIN) {
//        joinCost + (additionalRangeCost * (count - 1))
//      } else {
//        1
//      }
//
//      // scale attribute cost by expected cardinality
//      hints.cardinality(descriptor) match {
//        case Cardinality.HIGH    => 1 * multiplier
//        case Cardinality.UNKNOWN => 101 * multiplier
//        case Cardinality.LOW     => Int.MaxValue
//      }
//    }.sum
//    if (cost == 0) Int.MaxValue else cost // cost == 0 if somehow the filters don't match anything
    //    retrieveTableSize(getTableName(query.getTypeName, RecordTable))
  }

  override def close(): Unit = {
    shutdown.set(true)
    es.shutdown()
  }

  /**
   * Writes spatial bounds for this feature
   *
   * @param typeName simple feature type
   * @param bounds partial bounds - existing bounds will be expanded to include this
   */
  def writeSpatialBounds(typeName: String, bounds: Envelope): Unit = {
    val toWrite = readSpatialBounds(typeName) match {
      case Some(current) if current == bounds => None
      case _                                  => Some(bounds)
    }
    toWrite.foreach(b => ds.metadata.insert(typeName, SPATIAL_BOUNDS_KEY, encode(b)))
  }

  /**
   * Writes temporal bounds for this feature
   *
   * @param typeName simple feature type
   * @param bounds partial bounds - existing bounds will be expanded to include this
   */
  def writeTemporalBounds(typeName: String, bounds: Interval): Unit = {
    val toWrite = readTemporalBounds(typeName) match {
      case Some(current) if current == bounds => None
      case _                                  => Some(bounds)
    }
    toWrite.foreach(b => ds.metadata.insert(typeName, TEMPORAL_BOUNDS_KEY, encode(b)))
  }

  private def readTemporalBounds(typeName: String): Option[Interval] =
    ds.metadata.read(typeName, TEMPORAL_BOUNDS_KEY, cache = false).filterNot(_.isEmpty).map(decodeTimeBounds)

  private def readSpatialBounds(typeName: String): Option[Envelope] =
    ds.metadata.read(typeName, SPATIAL_BOUNDS_KEY, cache = false).filterNot(_.isEmpty).map(decodeSpatialBounds)

  // This lazily computed function helps shortcut getCount from scanning entire tables.
  private lazy val retrieveTableSize: (String) => Long =
    if (ds.connector.isInstanceOf[MockConnector]) {
      (tableName: String) => -1
    } else {
      val masterClient = MasterClient.getConnection(ds.connector.getInstance())
      val tc = new TCredentials()
      // TODO this will get stale, no?
      val mmi = masterClient.getMasterStats(Tracer.traceInfo(), tc)
      (tableName: String) => {
        val tableId = Tables.getTableId(ds.connector.getInstance(), tableName)
        val v = mmi.getTableMap.get(tableId)
        v.getRecs
      }
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

    import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator.decodeStat
    import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    val sft = ds.getSchema(typeName)
    val geom = Option(sft.getGeomField)

    val geomHistogram = geom.map(g => s"RangeHistogram($g,$GeometryHistogramSize,'POINT(-180 -90)','POINT(180 90)')")

    val dateHistogram = for {
      dtg <- sft.getDtgField
      bounds <- readTemporalBounds(typeName)
      weeks = bounds.toDuration.getStandardDays / 7
      if weeks > 1
    } yield {
      val start = GeoToolsDateFormat.print(bounds.getStart)
      val end = GeoToolsDateFormat.print(bounds.getEnd)
      s"RangeHistogram($dtg,$weeks,'$start','$end')"
    }

    val minMax = {
      val attrs = sft.getAttributeDescriptors.filter(_.isIndexed).map(_.getLocalName) ++ sft.getDtgField
      val toMinMax = geom.map(g => attrs.filter(_ != g)).getOrElse(attrs).distinct
      toMinMax.map(a => s"MinMax($a)")
    }

    val statStrings = geomHistogram.toSeq ++ dateHistogram ++ minMax

    if (statStrings.isEmpty) {
      logger.debug(s"Not calculating any stats for sft ${DataUtilities.encodeType(sft)}")
      return
    }

    logger.debug(s"Calculating stats for ${sft.getTypeName}: $statStrings")

    val stats = {
      val query = new Query(typeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.STATS_KEY, statStrings.mkString(";"))
      query.getHints.put(QueryHints.RETURN_ENCODED_KEY, java.lang.Boolean.TRUE)

      val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      try {
        decodeStat(reader.next.getAttribute(0).asInstanceOf[String], sft)
      } finally {
        reader.close()
      }
    }

    // match our return with what we passed in


//    "MinMax(attr);EnumeratedHistogram(idt);RangeHistogram(idt,5,10,15)"
//
//    val seqStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[SeqStat]
//    val stats = seqStat.stats
//    stats.size mustEqual 4
//
//    val minMax = stats(0).asInstanceOf[MinMax[java.lang.Long]]
//    val eh = stats(2).asInstanceOf[EnumeratedHistogram[java.lang.Integer]]
//    val rh = stats(3).asInstanceOf[RangeHistogram[java.lang.Integer]]
//
//    val rh = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[RangeHistogram[java.lang.Integer]]
//    rh.histogram.size mustEqual 5
//    rh.histogram(10) mustEqual 1
//    rh.histogram(11) mustEqual 1
//    rh.histogram(12) mustEqual 1
//    rh.histogram(13) mustEqual 1
//    rh.histogram(14) mustEqual 1
//    val minMaxStat = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[MinMax[java.lang.Long]]
//    minMaxStat.min mustEqual 0
//    minMaxStat.max mustEqual 298
//    val eh = decodeStat(sf.getAttribute(STATS).asInstanceOf[String]).asInstanceOf[EnumeratedHistogram[java.lang.Integer]]
//    eh.frequencyMap.size mustEqual 150
//    eh.frequencyMap(0) mustEqual 1
//    eh.frequencyMap(149) mustEqual 1
//    eh.frequencyMap(150) mustEqual 0

    true
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
  private def setLastUpdate(typeName: String, update: DateTime = DateTime.now(DateTimeZone.UTC)): Unit = {
    ds.metadata.insert(typeName, STATS_GENERATION_KEY, dtFormat.print(update))
  }

  /**
    * Reads the update interval.
    *
    * Note: will write the default update interval if the data doesn't exist
    *
    * @param typeName simple feature type name
    * @return update interval, in minutes
    */
  private def getUpdateInterval(typeName: String): Int = {
    ds.metadata.read(typeName, STATS_INTERVAL_KEY, cache = false) match {
      case Some(dt) => dt.toInt
      case None     =>
        // write the default so that it's there if anyone wants to modify it
        ds.metadata.insert(typeName, STATS_INTERVAL_KEY, DefaultUpdateInterval.toString)
        DefaultUpdateInterval
    }
  }
}

object GeoMesaStats {

  val dtFormat = ISODateTimeFormat.dateTime().withZoneUTC()

  val DefaultUpdateInterval = 360 // in minutes
  val GeometryHistogramSize = 1024 // corresponds to 2 digits of geohash

  def lockKey(table: String, typeName: String) = {
    val safeName = GeoMesaTable.hexEncodeNonAlphaNumeric(typeName)
    s"/org.locationtech.geomesa/accumulo/stats/$table/$safeName"
  }

  def allTimeBounds = new Interval(0L, System.currentTimeMillis(), DateTimeZone.UTC) // Epoch till now

  def decodeTimeBounds(value: String): Interval = {
    val longs = value.split(":").map(java.lang.Long.parseLong)
    require(longs(0) <= longs(1))
    require(longs.length == 2)
    new Interval(longs(0), longs(1), DateTimeZone.UTC)
  }

  def decodeSpatialBounds(string: String): Envelope = {
    val minMaxXY = string.split(":")
    require(minMaxXY.size == 4)
    new Envelope(minMaxXY(0).toDouble, minMaxXY(1).toDouble, minMaxXY(2).toDouble, minMaxXY(3).toDouble)
  }

  def encode(bounds: Interval): String = s"${bounds.getStartMillis}:${bounds.getEndMillis}"

  def encode(bounds: Envelope): String =
    Seq(bounds.getMinX, bounds.getMaxX, bounds.getMinY, bounds.getMaxY).mkString(":")
}
