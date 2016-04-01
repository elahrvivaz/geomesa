/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.geotools.data.{DataUtilities, Query, Transaction}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time._
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, GeoMesaTable, Z3Table}
import org.locationtech.geomesa.accumulo.index.{AttributeIdxStrategy, QueryHints, RecordIdxStrategy}
import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.{BoundsFilterVisitor, QueryPlanFilterVisitor}
import org.locationtech.geomesa.utils.geohash.{GeoHash, GeohashUtils}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

object GeoMesaMetadataStats {

  val DefaultHistogramSize  = 100  // how many buckets to sort each attribute into
  val GeometryHistogramPrecision = 2 // 2 digits of geohash - ~1252 km lon ~624 km lat
  val GeometryHistogramSize = math.pow(2, GeometryHistogramPrecision * 5).toInt

  val MinGeom = WKTUtils.read("POINT (-180 -90)")
  val MaxGeom = WKTUtils.read("POINT (180 90)")

  val CombinerName = "stats-combiner"

  private val serializers = scala.collection.mutable.Map.empty[String, StatSerializer]

  def serializer(sft: SimpleFeatureType): StatSerializer =
    serializers.synchronized(serializers.getOrElseUpdate(sft.getTypeName, StatSerializer(sft)))

  /**
    * Configures the stat combiner on the catalog table to sum stats dynamically.
    *
    * Note: should be called in a distributed lock on the catalog table
    * Note: this will need to be called again after a modifySchema call, when we implement that
    *
    * @param tableOps table operations
    * @param table catalog table
    * @param sft simple feature type
    */
  def configureStatCombiner(tableOps: TableOperations, table: String, sft: SimpleFeatureType): Unit = {

    import scala.collection.JavaConversions._

    // cols is the format expected by Combiners: 'cf1:cq1,cf2:cq2'
    def attach(options: Map[String, String]): Unit = {
      // priority needs to be less than the versioning iterator at 20
      val is = new IteratorSetting(10, CombinerName, classOf[StatsCombiner])
      options.foreach { case (k, v) => is.addOption(k, v) }
      tableOps.attachIterator(table, is)
    }

    val count = STATS_TOTAL_COUNT_KEY
    val attributes = statAttributesFor(sft)
    val minMax = attributes.map(minMaxKey)
    val histograms = attributes.map(histogramKey)

    // corresponds to metadata key (CF with empty CQ)
    val cols = (Seq(count) ++ minMax ++ histograms).map(k => s"$k:")
    val sftKey = s"${StatsCombiner.SftOption}${sft.getTypeName}"
    val sftOpt = SimpleFeatureTypes.encodeType(sft)

    val existing = tableOps.getIteratorSetting(table, CombinerName, IteratorScope.scan)
    if (existing == null) {
      attach(Map(sftKey -> sftOpt, "columns" -> cols.mkString(",")))
    } else {
      val existingSfts = existing.getOptions.filter(_._1.startsWith(StatsCombiner.SftOption))
      val existingCols = existing.getOptions.get("columns").split(",")
      if (!cols.forall(existingCols.contains) || !existingSfts.get(sftKey).contains(sftOpt)) {
        tableOps.removeIterator(table, CombinerName, java.util.EnumSet.allOf(classOf[IteratorScope]))
        val newCols = (cols ++ existingCols).distinct.mkString(",")
        attach(existingSfts.toMap ++ Map(sftKey -> sftOpt, "columns" -> newCols))
      }
    }
  }

  private [stats] def statAttributesFor(sft: SimpleFeatureType): Seq[String] = {
    import scala.collection.JavaConversions._
    val indexed = sft.getAttributeDescriptors.filter(okForStats).map(_.getLocalName)
    (Option(sft.getGeomField).toSeq ++ sft.getDtgField ++ indexed).distinct
  }

  // gets the key for storing a min-max attribute
  private [stats] def minMaxKey(attribute: String): String =
    s"$STATS_BOUNDS_PREFIX-${GeoMesaTable.hexEncodeNonAlphaNumeric(attribute)}"

  // gets the key for storing a histogram attribute
  private [stats] def histogramKey(attribute: String): String =
    s"$STATS_HISTOGRAM_PREFIX-${GeoMesaTable.hexEncodeNonAlphaNumeric(attribute)}"

  // determines if it is possible to run a min/max and histogram on the attribute
  private def okForStats(d: AttributeDescriptor): Boolean =
    d.isIndexed && !d.isMultiValued && d.getType.getBinding != classOf[java.lang.Boolean]

}

/**
 * Tracks stats via entries stored in metadata.
 *
 * Before updating stats, acquires a distributed lock to ensure that we aren't
 * duplicating effort.
 */
class GeoMesaMetadataStats(val ds: AccumuloDataStore) extends GeoMesaStats with LazyLogging {

  import GeoMesaMetadataStats._

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Long = {
    import scala.collection.JavaConversions._

    if (exact) {
      runStatQuery[CountStat](sft, Stat.Count(), filter).count
//      if (!sft.isPoints) {
//        // TODO stat query doesn't entirely handle duplicates - only on a per-iterator basis
//        // is a full scan worth it? the stat will be pretty close...
//
//        import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
//
//        // restrict fields coming back so that we push as little data as possible
//        val props = Array(Option(sft.getGeomField).getOrElse(sft.getDescriptor(0).getLocalName))
//        val query = new Query(sft.getTypeName, filter, props)
//        // length of an iterator is an int... this is Big Data
//        var count = 0L
//        SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).foreach(_ => count += 1)
//        count
//      }
    } else {
      import Filter.{EXCLUDE, INCLUDE}
      import org.locationtech.geomesa.filter.filterToString
      import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

      filter match {
        case EXCLUDE => 0
        case INCLUDE => readStat[CountStat](sft, STATS_TOTAL_COUNT_KEY).map(_.count).getOrElse(-1)

        case a: And => a.getChildren.map(getCount(sft, _, exact)).filter(_ != -1).minOrElse(-1)
        case o: Or  => o.getChildren.map(getCount(sft, _, exact)).filter(_ != -1).sumOrElse(-1)
        case i: Id  => RecordIdxStrategy.intersectIdFilters(Seq(i)).size
        case n: Not =>
          val neg = getCount(sft, n.getFilter, exact)
          if (neg == -1) -1 else readStat[CountStat](sft, STATS_TOTAL_COUNT_KEY).map(_.count - neg).getOrElse(-1)

        case _ =>
          val properties = DataUtilities.propertyNames(filter, sft).map(_.getPropertyName)
          if (properties.isEmpty || properties.size > 1) {
            logger.debug(s"Could not detect single property name in filter '${filterToString(filter)}'")
            -1
          } else {
            lazy val safeFilter = filter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter]
            val hasHistogram = Option(sft.getDescriptor(properties.head)).filter { ad =>
              ad.getLocalName == sft.getGeomField || sft.getDtgField.contains(ad.getLocalName) || ad.isIndexed
            }
            hasHistogram.flatMap(getHistogramCount(sft, _, safeFilter)).getOrElse(-1)
          }
      }
    }
  }

  /**
    * Estimates a count by checking saved histograms
    *
    * @param sft simple feature type
    * @param ad attribute descriptor being queried
    * @param filter filter to evaluate - epected to operate on the provided attribute
    * @return estimated count if available
    */
  private def getHistogramCount(sft: SimpleFeatureType, ad: AttributeDescriptor, filter: Filter): Option[Long] = {
    import GeohashUtils.{getUniqueGeohashSubstringsInPolygon => getGeohashes}
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.TryWithWarning

    def tryOption[T](f: => T, msg: => String) = TryWithWarning(f)(logger, msg).toOption

    val attribute = ad.getLocalName

    if (attribute == sft.getGeomField) {
      lazy val error = s"Failed to extract geohashes from filter ${filterToString(filter)}"
      for {
        histogram <- getHistogram[Geometry](sft, sft.getGeomField)
        geometry  <- tryOption(FilterHelper.extractSingleGeometry(Seq(filter)), error)
        geohashes <- tryOption(getGeohashes(geometry, 0, GeometryHistogramPrecision, includeDots = false), error)
      } yield {
        geohashes.map(gh => histogram.count(histogram.indexOf(GeoHash(gh).getPoint))).sum
      }
    } else {
      lazy val error = s"Failed to extract bounds from filter ${filterToString(filter)}"
      for {
        histogram <- getHistogram[Any](sft, attribute)
        bounds    <- tryOption(AttributeIdxStrategy.getBounds(sft, filter, None), error).map(_.bounds)
      } yield {
        val lower = bounds._1.map(v => AttributeTable.convertType(v, v.getClass, ad.getType.getBinding))
        val upper = bounds._2.map(v => AttributeTable.convertType(v, v.getClass, ad.getType.getBinding))
        if (upper.exists(histogram.isBelow) || lower.exists(histogram.isAbove)) { 0 } else {
          val lowerIndex = lower.map(histogram.indexOf).filter(_ != -1).getOrElse(0)
          val upperIndex = upper.map(histogram.indexOf).filter(_ != -1).getOrElse(histogram.length - 1)
          (lowerIndex to upperIndex).map(histogram.count).sum
        }
      }
    }
  }

  override def getBounds(sft: SimpleFeatureType, filter: Filter, exact: Boolean): ReferencedEnvelope = {
    val filterBounds = BoundsFilterVisitor.visit(filter)
    Option(sft.getGeomField).flatMap(getMinMax[Geometry](sft, _, filter, exact).bounds) match {
      case None => filterBounds
      case Some((min, max)) =>
        val env = min.getEnvelopeInternal
        env.expandToInclude(max.getEnvelopeInternal)
        filterBounds.intersection(env)
    }
  }

  override def getMinMax[T](sft: SimpleFeatureType, attribute: String, filter: Filter, exact: Boolean): MinMax[T] = {
    if (exact) {
      runStatQuery[MinMax[T]](sft, Stat.MinMax(attribute), filter)
    } else {
      // read stat if available or return an 'empty' stat
      readStat[MinMax[T]](sft, GeoMesaMetadataStats.minMaxKey(attribute))
          .getOrElse(Stat(sft, Stat.MinMax(attribute)).asInstanceOf[MinMax[T]])
    }
  }

  override def getHistogram[T](sft: SimpleFeatureType, attribute: String): Option[RangeHistogram[T]] =
    readStat[RangeHistogram[T]](sft, histogramKey(attribute))

  override def runStatQuery[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): T = {
    val query = new Query(sft.getTypeName, filter)
    query.getHints.put(QueryHints.STATS_KEY, stats)
    query.getHints.put(QueryHints.RETURN_ENCODED_KEY, java.lang.Boolean.TRUE)

    val reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    try {
      // stats should always return exactly one result, even if there are no features in the table
      KryoLazyStatsIterator.decodeStat(reader.next.getAttribute(0).asInstanceOf[String], sft).asInstanceOf[T]
    } finally {
      reader.close()
    }
  }

  override def runStats(sft: SimpleFeatureType): Stat = {
    import org.locationtech.geomesa.utils.geotools.{GeoToolsDateFormat => df}

    // calculate the stats we'll be gathering based on the simple feature type attributes
    val statString = buildStatsFor(sft)

    logger.debug(s"Calculating stats for ${sft.getTypeName}: $statString")

    val stats = runStatQuery[Stat](sft, statString)

    logger.debug(s"Writing stats for ${sft.getTypeName}")
    logger.trace(s"Stats for ${sft.getTypeName}: ${stats.toJson}")

    writeStat(stats, sft, merge = false) // don't merge, this is the authoritative value
    // update our last run time
    ds.metadata.insert(sft.getTypeName, STATS_GENERATION_KEY, df.print(DateTime.now(DateTimeZone.UTC)))

    stats
  }

  override def statUpdater(sft: SimpleFeatureType): StatUpdater = {
    val statString = buildStatsFor(sft)
    val tracker: () => Stat = () => Stat(sft, statString)
    new MetadataStatUpdater(this, sft, tracker)
  }

  /**
    * Write a stat to accumulo. If update == true, will attempt to merge the existing stat
    * and the new one, otherwise will overwrite.
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param merge merge with the existing stat - otherwise overwrite
    */
  private [stats] def writeStat(stat: Stat, sft: SimpleFeatureType, merge: Boolean): Unit = {
    stat match {
      case s: MinMax[_]         => writeStat(s, sft, minMaxKey(sft.getDescriptor(s.attribute).getLocalName), merge)
      case s: RangeHistogram[_] => writeStat(s, sft, histogramKey(sft.getDescriptor(s.attribute).getLocalName), merge)
      case s: CountStat         => writeStat(s, sft, STATS_TOTAL_COUNT_KEY, merge)
      case s: SeqStat           => s.stats.foreach(writeStat(_, sft, merge))
      case _ => throw new NotImplementedError("Only Count, MinMax and RangeHistogram stats are tracked")
    }
  }

  /**
    * Writes a stat to accumulo. We use a combiner to merge values in accumulo, so we don't have to worry
    * about synchronization or anything here.
    *
    * @param stat stat to write
    * @param sft simple feature type
    * @param key unique key for identifying the stat
    * @param merge merge with existing stat - otherwise overwrite
    */
  private def writeStat(stat: Stat, sft: SimpleFeatureType, key: String, merge: Boolean): Unit = {
    lazy val value = serializer(sft).serialize(stat)

    if (merge && stat.isInstanceOf[CountStat]) {
      // don't bother reading the existing value - we don't filter based on whether it's changed since it's additive
      ds.metadata.insert(sft.getTypeName, key, value)
    } else {
      // only re-write if it's changed - writes and compactions are expensive
      if (!readStat[Stat](sft, key).contains(stat)) {
        if (!merge) {
          ds.metadata.remove(sft.getTypeName, key)
        }
        ds.metadata.insert(sft.getTypeName, key, value)
      }
    }
  }

  /**
    * Read stat from accumulo
    *
    * @param sft simple feature type
    * @param key metadata key
    * @tparam T stat type
    * @return stat if it exists
    */
  private def readStat[T <: Stat](sft: SimpleFeatureType, key: String): Option[T] = {
    val raw = ds.metadata.read(sft.getTypeName, key, cache = false)
    val deserialized = raw.map(v => serializer(sft).deserialize(v))
    deserialized.collect { case s: T if !s.isEmpty => s }
  }

  /**
    * Determines the stats to calculate for a given schema
    *
    * @param sft simple feature type
    * @return stat string
    */
  private def buildStatsFor(sft: SimpleFeatureType): String = {
    import GeoMesaMetadataStats._

    val attributes = statAttributesFor(sft)

    val count = Stat.Count()
    val minMax = attributes.map(Stat.MinMax)
    val histograms = attributes.flatMap { attribute =>
      if (attribute == sft.getGeomField) {
        Seq(Stat.RangeHistogram(attribute, GeometryHistogramSize, MinGeom, MaxGeom))
      } else if (sft.getDtgField.contains(attribute)) {
        val hist = readStat[MinMax[Date]](sft, minMaxKey(attribute)).flatMap(_.bounds).map { bounds =>
          val minDt = new DateTime(bounds._1, DateTimeZone.UTC)
          // project two days into the future to account for new data being written
          val maxDt = new DateTime(bounds._2, DateTimeZone.UTC).plusDays(2)
          // offset our start to align with the z3 week splits
          val startWeek = Weeks.weeksBetween(Z3Table.EPOCH, minDt).getWeeks
          // add one to reach end of the week
          val endWeek = Weeks.weeksBetween(Z3Table.EPOCH, maxDt).getWeeks + 1

          val start = Z3Table.EPOCH.plusWeeks(startWeek).toDate
          val end   = Z3Table.EPOCH.plusWeeks(endWeek).toDate
          val weeks = endWeek - startWeek

          Stat.RangeHistogram(attribute, weeks, start, end)
        }
        hist.toSeq
      } else {
        val mm = readStat[MinMax[Any]](sft, minMaxKey(attribute))
        // we need the class-tag binding to correctly create the stat
        val hist = mm.flatMap(_.bounds).filter(b => b._1 != b._2).collect {
          case (s: String,  e: String)  => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: Integer, e: Integer) => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: jLong,   e: jLong)   => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: jFloat,  e: jFloat)  => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: jDouble, e: jDouble) => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
          case (s: Date,    e: Date)    => Stat.RangeHistogram(attribute, DefaultHistogramSize, s, e)
        }
        hist.toSeq
      }
    }

    Stat.SeqStat(Seq(count) ++ minMax ++ histograms)
  }
}

/**
  * Stores stats as metadata entries
  *
  * @param stats persistence
  * @param sft simple feature type
  * @param statFunction creates stats for tracking new features
  */
class MetadataStatUpdater(stats: GeoMesaMetadataStats, sft: SimpleFeatureType, statFunction: () => Stat)
    extends StatUpdater with LazyLogging {

  private var stat: Stat = statFunction()

  override def add(sf: SimpleFeature): Unit = stat.observe(sf)

  override def remove(sf: SimpleFeature): Unit = stat.unobserve(sf)

  override def close(): Unit = stats.writeStat(stat, sft, merge = true)

  override def flush(): Unit = {
    stats.writeStat(stat, sft, merge = true)
    // reload the tracker - for long-held updaters, this will refresh the date histogram range
    stat = statFunction()
  }
}
