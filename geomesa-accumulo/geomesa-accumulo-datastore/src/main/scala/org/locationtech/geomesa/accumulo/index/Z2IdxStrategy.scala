/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.index

import com.google.common.base.Charsets
import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.data.Range
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.tables.{Z2Table, Z3Table}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.{Z2SFC, Z3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class Z2IdxStrategy(val filter: QueryFilter) extends Strategy with Logging with IndexFilterHelpers  {

  import FilterHelper._
  import Z3IdxStrategy._

  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    val dtgField = sft.getDtgField

    val (geomFilters, temporalFilters) = filter.primary.partition(isSpatialFilter)
    val ecql = filter.secondary
    val allFilter = Some(filter.filter).filter(_ != Filter.INCLUDE)

    output(s"Geometry filters: ${filtersToString(geomFilters)}")
    output(s"Temporal filters: ${filtersToString(temporalFilters)}")

    val tweakedGeomFilters = geomFilters.map(updateTopologicalFilters(_, sft))

    output(s"Tweaked geom filters are $tweakedGeomFilters")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tweakedGeomFilters.flatMap(decomposeToGeometry)

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val interval = extractInterval(temporalFilters, dtgField)
    val geometryToCover = netGeom(collectionToCover)

    output(s"GeomsToCover: $geometryToCover")
    output(s"Interval:  $interval")

    val fp = FILTERING_ITER_PRIORITY
// TODO apply filter
    val (iterators, kvsToFeatures, colFamily) = if (hints.isBinQuery) {
      // TODO
      val trackId = hints.getBinTrackIdField
      val geom = hints.getBinGeomField
      val dtg = hints.getBinDtgField
      val label = hints.getBinLabelField

      val batchSize = hints.getBinBatchSize
      val sort = hints.isBinSorting
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (ecql.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, trackId, geom, dtg, label)) {
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, ecql, batchSize, sort, fp)), Z3Table.BIN_CF)
        } else {
          val binDtg = dtg.getOrElse(dtgField.get) // dtgField is always defined if we're using z3
          val binGeom = geom.getOrElse(sft.getGeomField)
          val iter = BinAggregatingIterator.configureDynamic(sft, ecql, trackId, binGeom, binDtg, label,
            batchSize, sort, fp)
          (Seq(iter), Z2Table.FULL_CF)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf)
    } else if (hints.isDensityQuery) {
      val envelope = hints.getDensityEnvelope.get
      val (width, height) = hints.getDensityBounds.get
      val weight = hints.getDensityWeight
      // TODO allow for non point geoms in the iter
      val iter = Z3DensityIterator.configure(sft, allFilter, envelope, width, height, weight, fp)
      (Seq(iter), Z3DensityIterator.kvsToFeatures(), Z2Table.FULL_CF)
    } else {
      val transforms = for {
        tdef <- hints.getTransformDefinition
        tsft <- hints.getTransformSchema
      } yield { (tdef, tsft) }
      output(s"Transforms: $transforms")

      val iters = (allFilter, transforms) match {
        case (None, None) => Seq.empty
        case _ => Seq(KryoLazyFilterTransformIterator.configure(sft, allFilter, transforms, fp))
      }
      (iters, queryPlanner.defaultKVsToFeatures(hints), Z2Table.FULL_CF)
    }

    val table = acc.getTableName(sft.getTypeName, Z2Table)
    val numThreads = acc.getSuggestedThreads(sft.getTypeName, Z2Table)

    // setup iterator
    val env = geometryToCover.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

    val startTime = Z2Table.encodeTime(interval.getStartMillis)
    val endTime = Z2Table.encodeTime(interval.getEndMillis)
    val prefix = sft.getTableSharingPrefix.getBytes(Charsets.UTF_8)

    val ranges = Z2SFC.ranges((lx, ux), (ly, uy), 32).flatMap { case (lo, hi) =>
      val loBytes = Longs.toByteArray(lo).take(4)
      val hiBytes = Longs.toByteArray(hi).take(4)
      Z2Table.SHARDS.map { s =>
        val start = prefix ++ Seq(s) ++ loBytes ++ startTime
        val end = prefix ++ Seq(s) ++ hiBytes ++ endTime
        new Range(new Text(start), true, Range.followingPrefix(new Text(end)), false)
      }
    }

    val hasDupes = false // TODO may contain dupes
    if (hasDupes) {
      // TODO add 'dot' ranges
    }
    BatchScanPlan(table, ranges, iterators, Seq(colFamily), kvsToFeatures, numThreads, hasDupes)
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

object Z2IdxStrategy extends StrategyProvider {

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
}
