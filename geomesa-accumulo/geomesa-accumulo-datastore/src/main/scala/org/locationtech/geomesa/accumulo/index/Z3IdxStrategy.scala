/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.index

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.data.Range
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.joda.time.Weeks
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.spatial._

class Z3IdxStrategy(val filter: QueryFilter) extends Strategy with Logging with IndexFilterHelpers  {

  import FilterHelper._
  import Z3IdxStrategy._

  /**
   * Plans the query - strategy implementations need to define this
   */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    val dtgField = sft.getDtgField

    val (geomFilters, temporalFilters) = {
      val (g, t) = filter.primary.partition(isSpatialFilter)
      if (g.isEmpty) {
        // allow for date only queries - if no geom, use whole world
        (Seq(ff.bbox(sft.getGeomField, -180, -90, 180, 90, "EPSG:4326")), t)
      } else {
        (g, t)
      }
    }

    output(s"Geometry filters: ${filtersToString(geomFilters)}")
    output(s"Temporal filters: ${filtersToString(temporalFilters)}")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tryReduceGeometryFilter(geomFilters).flatMap(decomposeToGeometry)

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    // since we don't apply a temporal filter, we pass offsetDuring to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val interval = extractInterval(temporalFilters, dtgField, exclusive = true)
    val geometryToCover = netGeom(collectionToCover)

    output(s"GeomsToCover: $geometryToCover")
    output(s"Interval:  $interval")

    val fp = FILTERING_ITER_PRIORITY

    // If we have some sort of complicated geometry predicate,
    // we need to pass it through to be evaluated
    val appliedGeomFilter: Option[Filter]  = filterListAsAnd(geomFilters.filter(isComplicatedSpatialFilter))

    val ecql: Option[Filter] = (appliedGeomFilter, filter.secondary) match {
      case (None, fs)           => fs
      case (gf, None)           => gf
      case (Some(gf), Some(fs)) => Some(ff.and(gf, fs))
    }

    val (iterators, kvsToFeatures, colFamily) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (ecql.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, ecql, hints)), Z3Table.BIN_CF)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, ecql, hints)
          (Seq(iter), Z3Table.FULL_CF)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf)
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(sft, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), Z3Table.FULL_CF)
    } else if (hints.isTemporalDensityQuery) {
      val iter = KryoLazyTemporalDensityIterator.configure(sft, ecql, hints)
      (Seq(iter), queryPlanner.defaultKVsToFeatures(hints), Z3Table.FULL_CF)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, ecql, hints)
      (Seq(iter), queryPlanner.defaultKVsToFeatures(hints), Z3Table.FULL_CF)
    } else {
      val transforms = for {
        tdef <- hints.getTransformDefinition
        tsft <- hints.getTransformSchema
      } yield { (tdef, tsft) }
      output(s"Transforms: $transforms")

      val iters = (ecql, transforms) match {
        case (None, None) => Seq.empty
        case _ => Seq(KryoLazyFilterTransformIterator.configure(sft, ecql, transforms, fp))
      }
      (iters, Z3Table.adaptZ3KryoIterator(hints.getReturnSft), Z3Table.FULL_CF)
    }

    val z3table = acc.getTableName(sft.getTypeName, Z3Table)
    val numThreads = acc.getSuggestedThreads(sft.getTypeName, Z3Table)

    // setup Z3 iterator
    val env = geometryToCover.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

    val epochWeekStart = Weeks.weeksBetween(Z3Table.EPOCH, interval.getStart)
    val epochWeekEnd = Weeks.weeksBetween(Z3Table.EPOCH, interval.getEnd)
    val weeks = scala.Range.inclusive(epochWeekStart.getWeeks, epochWeekEnd.getWeeks)
    val lt = Z3Table.secondsInCurrentWeek(interval.getStart, epochWeekStart)
    val ut = Z3Table.secondsInCurrentWeek(interval.getEnd, epochWeekEnd)

    val lz = Z3SFC.index(lx, ly, lt).z
    val uz = Z3SFC.index(ux, uy, ut).z

    // the z3 index breaks time into 1 week chunks, so create a range for each week in our range
    val (ranges, zMap) = if (weeks.length == 1) {
      val ranges = getRanges(weeks, (lx, ux), (ly, uy), (lt, ut))
      val map = Map(weeks.head.toShort -> (lz, uz))
      (ranges, map)
    } else {
      // time range for a chunk is 0 to 1 week (in seconds)
      val tMax = Weeks.ONE.toStandardSeconds.getSeconds
      val head +: middle :+ last = weeks.toList
      val headRanges = getRanges(Seq(head), (lx, ux), (ly, uy), (lt, tMax))
      val lastRanges = getRanges(Seq(last), (lx, ux), (ly, uy), (0, ut))
      val middleRanges = if (middle.isEmpty) Seq.empty else getRanges(middle, (lx, ux), (ly, uy), (0, tMax))
      val ranges = headRanges ++ middleRanges ++ lastRanges
      val minz = Z3SFC.index(lx, ly, 0).z
      val maxZ = Z3SFC.index(ux, uy, tMax).z
      val map = Map(head.toShort -> (lz, maxZ), last.toShort -> (minz, uz)) ++
          middle.map(_.toShort -> (minz, maxZ)).toMap
      (ranges, map)
    }

    val zIter = Z3Iterator.configure(zMap, Z3_ITER_PRIORITY)
    val iters = Seq(zIter) ++ iterators
    BatchScanPlan(z3table, ranges, iters, Seq(colFamily), kvsToFeatures, numThreads, hasDuplicates = false)
  }

  def getRanges(weeks: Seq[Int], x: (Double, Double), y: (Double, Double), t: (Long, Long)): Seq[Range] = {
    val prefixes = weeks.map(w => Shorts.toByteArray(w.toShort))
    Z3SFC.ranges(x, y, t).flatMap { case (s, e) =>
      val startBytes = Longs.toByteArray(s)
      val endBytes = Longs.toByteArray(e)
      prefixes.map { prefix =>
        val start = new Text(Bytes.concat(prefix, startBytes))
        val end = Range.followingPrefix(new Text(Bytes.concat(prefix, endBytes)))
        new Range(start, true, end, false)
      }
    }
  }
}

object Z3IdxStrategy extends StrategyProvider {

  val Z3_ITER_PRIORITY = 21
  val FILTERING_ITER_PRIORITY = 25

  /**
   * Gets the estimated cost of running the query. Currently, cost is hard-coded to sort between
   * strategies the way we want. Z3 should be more than id lookups (at 1), high-cardinality attributes (at 1)
   * and less than STidx (at 400) and unknown cardinality attributes (at 999).
   *
   * Eventually cost will be computed based on dynamic metadata and the query.
   */
  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) =
    if (filter.primary.length > 1) 200 else 400

  def isComplicatedSpatialFilter(f: Filter): Boolean = !f.isInstanceOf[BBOX]
}
