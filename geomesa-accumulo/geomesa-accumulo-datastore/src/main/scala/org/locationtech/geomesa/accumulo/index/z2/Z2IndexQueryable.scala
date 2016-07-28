/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z2

import java.nio.charset.StandardCharsets

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Or}

object Z2IndexQueryable extends AccumuloIndexQueryable with LazyLogging {

  override def getQueryPlan(ds: AccumuloDataStore,
                            sft: SimpleFeatureType,
                            filter: FilterStrategy,
                            hints: Hints,
                            explain: ExplainerOutputType): QueryPlan = {

    import QueryHints.{LOOSE_BBOX, RichHints}
    import org.locationtech.geomesa.filter.FilterHelper._
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    explain(s"Geometries: $geometries")

    val looseBBox = if (hints.containsKey(LOOSE_BBOX)) Boolean.unbox(hints.get(LOOSE_BBOX)) else ds.config.looseBBox

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
      val (iters, cf) =
        if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, Z2Index, ecql, hints, sft.nonPoints)), Z2IndexWritable.BIN_CF)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, Z2Index, ecql, hints, sft.nonPoints)
          (Seq(iter), Z2IndexWritable.FULL_CF)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf, false)
    } else if (hints.isDensityQuery) {
      val iter = Z2DensityIterator.configure(sft, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), Z2IndexWritable.FULL_CF, false)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, Z2Index, ecql, hints, sft.nonPoints)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), Z2IndexWritable.FULL_CF, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, Z2Index, ecql, hints, sft.nonPoints)
      (Seq(iter), Z2IndexWritable.entriesToFeatures(sft, hints.getReturnSft), Z2IndexWritable.FULL_CF, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, ecql, hints).toSeq
      (iters, Z2IndexWritable.entriesToFeatures(sft, hints.getReturnSft), Z2IndexWritable.FULL_CF, sft.nonPoints)
    }

    val z2table = ds.getTableName(sft.getTypeName, Z2Index)
    val numThreads = ds.getSuggestedThreads(sft.getTypeName, Z2Index)

    val (ranges, z2Iter) = if (filter.primary.isEmpty) {
      val range = if (sft.isTableSharing) {
        aRange.prefix(new Text(sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)))
      } else {
        new aRange()
      }
      (Seq(range), None)
    } else {
      // setup Z2 iterator
      import Z2IndexWritable.GEOM_Z_NUM_BYTES
      val xy = geometries.map(GeometryUtils.bounds)
      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      val zRanges = if (sft.isPoints) {
        Z2SFC.ranges(xy, 64, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      } else {
        Z2SFC.ranges(xy, 8 * GEOM_Z_NUM_BYTES, rangeTarget).map { r =>
          (Longs.toByteArray(r.lower).take(GEOM_Z_NUM_BYTES), Longs.toByteArray(r.upper).take(GEOM_Z_NUM_BYTES))
        }
      }

      val prefixes = if (sft.isTableSharing) {
        val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
        Z2IndexWritable.SPLIT_ARRAYS.map(ts ++ _)
      } else {
        Z2IndexWritable.SPLIT_ARRAYS
      }

      val ranges = prefixes.flatMap { prefix =>
        zRanges.map { case (lo, hi) =>
          val start = new Text(Bytes.concat(prefix, lo))
          val end = aRange.followingPrefix(new Text(Bytes.concat(prefix, hi)))
          new aRange(start, true, end, false)
        }
      }

      val zIter = Z2Iterator.configure(xy, sft.isPoints, sft.isTableSharing, Z2_ITER_PRIORITY)

      (ranges, Some(zIter))
    }

    val perAttributeIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
    }
    val cf = if (perAttributeIter.isEmpty) colFamily else AccumuloFeatureIndex.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators ++ z2Iter
    BatchScanPlan(filter, z2table, ranges, iters, Seq(cf), kvsToFeatures, numThreads, hasDupes)
  }

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[FilterStrategy] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    if (filter == Filter.INCLUDE) {
      Seq(FilterStrategy(Z2Index, None, None))
    } else if (filter == Filter.EXCLUDE) {
      Seq.empty
    } else {
      val (spatial, nonSpatial) = FilterExtractingVisitor(filter, sft.getGeomField, sft, spatialCheck)
      if (spatial.nonEmpty) {
        Seq(FilterStrategy(Z2Index, spatial, nonSpatial))
      } else {
        Seq(FilterStrategy(Z2Index, None, Some(filter)))
      }
    }
  }


  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: FilterStrategy,
                       transform: Option[SimpleFeatureType]): Long = {
    filter.primary match {
      case None => Long.MaxValue
      // add one so that we prefer the z3 index even if geometry is the limiting factor, resulting in the same count
      case Some(f) =>
        stats.flatMap(_.getCount(sft, f, exact = false).map(c => if (c == 0L) 0L else c + 1L)).getOrElse(400L) // 400 = old index based cost
    }
  }

  val Z2_ITER_PRIORITY = 23
  val FILTERING_ITER_PRIORITY = 25

  /**
    * Evaluates filters that we can handle with the z-index strategies
    *
    * @param filter filter to check
    * @return
    */
  def spatialCheck(filter: Filter): Boolean = {
    filter match {
      case f: And => true // note: implies further evaluation of children
      case f: Or  => true // note: implies further evaluation of children
      case _ => isSpatialFilter(filter)
    }
  }
}
