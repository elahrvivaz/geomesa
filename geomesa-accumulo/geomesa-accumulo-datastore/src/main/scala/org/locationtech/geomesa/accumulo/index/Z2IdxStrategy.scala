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
import org.locationtech.geomesa.accumulo.data.tables.Z2Table
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.ZRange.ZPrefix
import org.locationtech.geomesa.curve.{Z2, Z2SFC, Z3SFC}
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

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = geomFilters.flatMap(decomposeToGeometry)

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val interval = extractInterval(temporalFilters, dtgField)
    val geometryToCover = netGeom(collectionToCover)

    output(s"GeomsToCover: $geometryToCover")
    output(s"Interval:  $interval")

    val fp = FILTERING_ITER_PRIORITY

    val (iterators, kvsToFeatures, colFamily) = if (hints.isBinQuery) {
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
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, allFilter, batchSize, sort, fp)), Z2Table.BIN_CF)
        } else {
          val binDtg = dtg.orElse(dtgField)
          val binGeom = geom.getOrElse(sft.getGeomField)
          val iter = BinAggregatingIterator.configureDynamic(sft, allFilter, trackId, binGeom, binDtg, label,
            batchSize, sort, fp)
          (Seq(iter), Z2Table.FULL_CF)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf)
    } else {
      val transforms =
        for {tdef <- hints.getTransformDefinition; tsft <- hints.getTransformSchema} yield {(tdef, tsft)}
      output(s"Transforms: $transforms")

      val (cfSft, cf) = if (IteratorTrigger.canUseIndexValues(sft, allFilter, transforms.map(_._2))) {
        (IndexValueEncoder.getIndexSft(sft), Z2Table.MAP_CF) // TODO cache this
      } else {
        (sft, Z2Table.FULL_CF)
      }
      if (hints.isDensityQuery) {
        val envelope = hints.getDensityEnvelope.get
        val (width, height) = hints.getDensityBounds.get
        val weight = hints.getDensityWeight
        val iter = KryoLazyDensityIterator.configure(cfSft, allFilter, envelope, width, height, weight, fp)
        (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), cf)
      } else {
        val iters = (allFilter, transforms) match {
          case (None, None) => Seq.empty
          case _ => Seq(KryoLazyFilterTransformIterator.configure(cfSft, allFilter, transforms, fp))
        }
        (iters, queryPlanner.defaultKVsToFeatures(hints), cf)
      }
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
      val pointRanges = Z2Table.SHARDS.map { s =>
        val start = Bytes.concat(prefix, s, loBytes, startTime)
        val end = Bytes.concat(prefix, s, hiBytes, endTime)
        new Range(new Text(start), true, Range.followingPrefix(new Text(end)), false)
      }
      def nonPointRanges = {
        val ZPrefix(zPrefix, bits) = Z2.zBox(Z2(lo), Z2(hi))
        val z2 = Longs.toByteArray(zPrefix).take(4)
        // flip the first 3 bits to indicate a non-point geom
        // these bits will always be 0 (unused) in the z2 value
        // bits flipped indicate the precision of the z value - creates a box
        val oneDot = if (bits > 32) { None } else {
          // first bit flipped, last byte zeroed
          Some(Array((z2.head | 0x80).toByte) ++ z2.tail.take(2) ++ Array[Byte](0))
        }
        val twoDots = if (bits > 24) { None } else {
          // first two bits flipped, last 2 bytes zeroed
          Some(Array((z2.head | 0xc0).toByte) ++ z2.tail.take(1) ++ Array[Byte](0, 0))
        }
        val threeDots = if (bits > 16) { None } else {
          // first, 3rd bit flipped, last 3 bytes zeroed
          Some(Array((z2.head | 0xa0).toByte) ++ Array[Byte](0, 0, 0))
        }
        val fourDots = if (bits > 8) { None } else {
          // first three bits flipped, everything else zeroed
          Some(Array[Byte](0xe0.toByte, 0, 0, 0))
        }
        (oneDot ++ twoDots ++ threeDots ++ fourDots).flatMap { zBytes =>
          Z2Table.SHARDS.map { s =>
            val row = Bytes.concat(prefix, s, zBytes)
            val start = Bytes.concat(row, startTime)
            val end = Bytes.concat(row, endTime)
            new Range(new Text(start), true, Range.followingPrefix(new Text(end)), false)
          }
        }
      }
      if (sft.isPoints) pointRanges else pointRanges ++ nonPointRanges
    }

    BatchScanPlan(table, ranges, iterators, Seq(colFamily), kvsToFeatures, numThreads, hasDuplicates = false)
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
