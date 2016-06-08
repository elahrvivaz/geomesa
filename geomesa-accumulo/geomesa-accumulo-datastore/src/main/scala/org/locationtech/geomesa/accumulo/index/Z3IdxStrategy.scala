/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeatureType

class Z3IdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging with IndexFilterHelpers  {

  /**
   * Plans the query - strategy implementations need to define this
   */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType): QueryPlan = {

    import QueryHints.{LOOSE_BBOX, RichHints}
    import Z3IdxStrategy._
    import Z3Table.GEOM_Z_NUM_BYTES
    import org.locationtech.geomesa.filter.FilterHelper._

    val ds  = queryPlanner.ds
    val sft = queryPlanner.sft

    // note: z3 requires a date field
    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute a z3 query but the schema does not have a date")
    }

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    // standardize the two key query arguments:  polygon and date-range

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField))
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals = filter.primary.map(extractIntervals(_, dtgField, handleExclusiveBounds = true)).getOrElse(Seq.empty)

    output(s"Geometries: $geometries")
    output(s"Intervals: $intervals")

    val looseBBox = if (hints.containsKey(LOOSE_BBOX)) Boolean.unbox(hints.get(LOOSE_BBOX)) else ds.config.looseBBox
    val hasSplits = Z3Table.hasSplits(sft)

    // if the user has requested strict bounding boxes, we apply the full filter
    // if this is a non-point geometry type, the index is coarse-grained, so we apply the full filter
    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    val ecql = if (looseBBox && sft.isPoints && geometries.forall(GeometryUtils.isRectangular)) {
      filter.secondary
    } else {
      filter.filter
    }

    val (iterators, kvsToFeatures, colFamily, hasDupes) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
        val idOffset = Z3Table.getIdRowOffset(sft)
        val iter = BinAggregatingIterator.configurePrecomputed(sft, ecql, hints, idOffset, sft.nonPoints)
        (Seq(iter), BinAggregatingIterator.kvsToFeatures(), Z3Table.BIN_CF, false)
      } else {
        val iter = BinAggregatingIterator.configureDynamic(sft, ecql, hints, sft.nonPoints)
        (Seq(iter), BinAggregatingIterator.kvsToFeatures(), Z3Table.FULL_CF, false)
      }
    } else if (hints.isDensityQuery) {
      val iter = Z3DensityIterator.configure(sft, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), Z3Table.FULL_CF, false)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, ecql, hints, sft.nonPoints)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), Z3Table.FULL_CF, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, ecql, hints, sft.nonPoints)
      (Seq(iter), queryPlanner.defaultKVsToFeatures(hints), Z3Table.FULL_CF, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, ecql, hints).toSeq
      (iters, queryPlanner.defaultKVsToFeatures(hints), Z3Table.FULL_CF, sft.nonPoints)
    }

    val z3table = ds.getTableName(sft.getTypeName, Z3Table)
    val numThreads = ds.getSuggestedThreads(sft.getTypeName, Z3Table)

    // compute our accumulo ranges based on the coarse bounds for our query
    val (ranges, zIterator) = if (filter.primary.isEmpty) { (Seq(new aRange()), None) } else {
      val xy = geometries.map { g =>
        val env = g.getEnvelopeInternal
        (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
      }

      // calculate map of weeks to time intervals in that week
      val timesByWeek = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
      // note: intervals shouldn't have any overlaps
      intervals.foreach { interval =>
        val (lWeek, lt) = Z3Table.getWeekAndSeconds(interval._1)
        val (uWeek, ut) = Z3Table.getWeekAndSeconds(interval._2)
        if (lWeek == uWeek) {
          timesByWeek(lWeek) ++= Seq((lt, ut))
        } else {
          timesByWeek(lWeek) ++= Seq((lt, MaxTime))
          timesByWeek(uWeek) ++= Seq((MinTime, ut))
          Range.inclusive(lWeek + 1, uWeek - 1).foreach { w => timesByWeek(w.toShort) = WholeWeek }
        }
      }

      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      def toZRanges(t: Seq[(Long, Long)]): Seq[(Array[Byte], Array[Byte])] = if (sft.isPoints) {
        Z3SFC.ranges(xy, t, 64, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      } else {
        Z3SFC.ranges(xy, t, 8 * GEOM_Z_NUM_BYTES, rangeTarget).map { r =>
          (Longs.toByteArray(r.lower).take(GEOM_Z_NUM_BYTES), Longs.toByteArray(r.upper).take(GEOM_Z_NUM_BYTES))
        }
      }

      lazy val wholeWeekRanges = toZRanges(WholeWeek)

      val ranges = timesByWeek.flatMap { case (w, times) =>
        val zs = if (times.eq(WholeWeek)) wholeWeekRanges else toZRanges(times)
        val wBytes = Shorts.toByteArray(w)
        val prefixes = if (hasSplits) Z3Table.SPLIT_ARRAYS.map(Bytes.concat(_, wBytes)) else Seq(wBytes)
        prefixes.flatMap { prefix =>
          zs.map { case (lo, hi) =>
            val start = Bytes.concat(prefix, lo)
            val end = Bytes.concat(prefix, hi)
            new aRange(new Text(start), true, aRange.followingPrefix(new Text(end)), false)
          }
        }
      }

      val zIter = Z3Iterator.configure(xy, timesByWeek.toMap, sft.isPoints, hasSplits, Z3_ITER_PRIORITY)
      (ranges.toSeq, Some(zIter))
    }

    val iters = zIterator.toSeq ++ iterators
    BatchScanPlan(filter, z3table, ranges, iters, Seq(colFamily), kvsToFeatures, numThreads, hasDupes)
  }
}

object Z3IdxStrategy extends StrategyProvider {

  val Z3_ITER_PRIORITY = 21
  val FILTERING_ITER_PRIORITY = 25

  val MinTime = Z3SFC.time.min.toLong
  val MaxTime = Z3SFC.time.max.toLong
  val WholeWeek = Seq((MinTime, MaxTime))

  override protected def statsBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType],
                                        stats: GeoMesaStats): Option[Long] = {
    // https://geomesa.atlassian.net/browse/GEOMESA-1166
    // TODO check date range and use z2 instead if too big
    // TODO also if very small bbox, z2 has ~10 more bits of lat/lon info
    filter.primary match {
      case Some(f) => stats.getCount(sft, f, exact = false)
      case None    => Some(Long.MaxValue)
    }
  }

  /**
    * More than id lookups (at 1), high-cardinality attributes (at 1).
    * Less than unknown cardinality attributes (at 999).
    * With a spatial component, less than z2, otherwise more than z2 (at 400)
    */
  override protected def indexBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType]): Long = {
    if (filter.primary.exists(isSpatialFilter)) 200L else 401L
  }
}
