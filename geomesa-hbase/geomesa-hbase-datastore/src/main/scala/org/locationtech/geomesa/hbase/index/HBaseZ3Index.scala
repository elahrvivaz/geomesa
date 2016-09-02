/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.hbase.index

import java.nio.charset.StandardCharsets
import java.util.Date

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Point
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.{Delete, Mutation, Put, Scan}
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object HBaseZ3Index extends HBaseFeatureIndex
    with SpatioTemporalFilterStrategy[HBaseDataStore, HBaseWritableFeature, Mutation, HBaseQueryPlan]
    with LazyLogging {

  override val name: String = "z3"
  override val version: Int = 1

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && sft.isPoints
  }

  override def configure(sft: SimpleFeatureType, ops: HBaseDataStore): Unit = {
    val name = getTableName(sft)
    if (!ops.connection.getAdmin.tableExists(name)) {
      val descriptor = new HTableDescriptor(name)
      descriptor.addFamily(HBaseFeatureIndex.DataColumnFamilyDescriptor)
      ops.connection.getAdmin.createTable(descriptor)
    }
  }

  override def writer(sft: SimpleFeatureType, ops: HBaseDataStore): (HBaseWritableFeature) => Mutation = {
    import HBaseFeatureIndex._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)

    (wf) => {
      val split = DefaultSplitArrays(wf.idHash % DefaultNumSplits)
      val (timeBin, z) = {
        val dtg = wf.feature.getAttribute(dtgIndex).asInstanceOf[Date]
        val time = if (dtg == null) 0 else dtg.getTime
        val BinnedTime(b, t) = timeToIndex(time)
        val geom = wf.feature.getDefaultGeometry.asInstanceOf[Point]
        (b, sfc.index(geom.getX, geom.getY, t).z)
      }
      val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
      val row = Bytes.concat(split, Shorts.toByteArray(timeBin), Longs.toByteArray(z), id)

      new Put(row).addImmutable(wf.fullValue.cf, wf.fullValue.cq, wf.fullValue.value)
    }
  }

  override def remover(sft: SimpleFeatureType, ops: HBaseDataStore): (HBaseWritableFeature) => Mutation = {
    import HBaseFeatureIndex._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)

    (wf) => {
      val split = DefaultSplitArrays(wf.idHash % DefaultNumSplits)
      val (timeBin, z) = {
        val dtg = wf.feature.getAttribute(dtgIndex).asInstanceOf[Date]
        val time = if (dtg == null) 0 else dtg.getTime
        val BinnedTime(b, t) = timeToIndex(time)
        val geom = wf.feature.getDefaultGeometry.asInstanceOf[Point]
        (b, sfc.index(geom.getX, geom.getY, t).z)
      }
      val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
      val row = Bytes.concat(split, Shorts.toByteArray(timeBin), Longs.toByteArray(z), id)

      new Delete(row).addFamily(wf.fullValue.cf)
    }
  }

  override def removeAll(sft: SimpleFeatureType, ops: HBaseDataStore): Unit = ???

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String =
    (row: Array[Byte]) => new String(row, 11, row.length - 11, StandardCharsets.UTF_8)

  override def getQueryPlan(sft: SimpleFeatureType,
                            ops: HBaseDataStore,
                            filter: TypedFilterStrategy,
                            hints: Hints,
                            explain: Explainer): HBaseQueryPlan = {
    import org.locationtech.geomesa.filter.FilterHelper._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

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

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals = filter.primary.map(extractIntervals(_, dtgField, handleExclusiveBounds = true)).getOrElse(Seq.empty)

    explain(s"Geometries: $geometries")
    explain(s"Intervals: $intervals")

    val sfc = Z3SFC(sft.getZ3Interval)
    val minTime = sfc.time.min.toLong
    val maxTime = sfc.time.max.toLong
    val wholePeriod = Seq((minTime, maxTime))

    // compute our accumulo ranges based on the coarse bounds for our query
    val ranges = if (filter.primary.isEmpty) {
      Seq(new Scan().addFamily(HBaseFeatureIndex.DataColumnFamily))
    } else {
      val xy = geometries.map(GeometryUtils.bounds)

      // calculate map of weeks to time intervals in that week
      val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
      val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
      // note: intervals shouldn't have any overlaps
      intervals.foreach { interval =>
        val BinnedTime(lb, lt) = dateToIndex(interval._1)
        val BinnedTime(ub, ut) = dateToIndex(interval._2)
        if (lb == ub) {
          timesByBin(lb) ++= Seq((lt, ut))
        } else {
          timesByBin(lb) ++= Seq((lt, maxTime))
          timesByBin(ub) ++= Seq((minTime, ut))
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = wholePeriod)
        }
      }

      val rangeTarget = Some(2000)
      def toZRanges(t: Seq[(Long, Long)]): Seq[(Array[Byte], Array[Byte])] =
        sfc.ranges(xy, t, 64, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))

      lazy val wholePeriodRanges = toZRanges(wholePeriod)

      val ranges = timesByBin.flatMap { case (b, times) =>
        val zs = if (times.eq(wholePeriod)) wholePeriodRanges else toZRanges(times)
        val binBytes = Shorts.toByteArray(b)
        val prefixes = HBaseFeatureIndex.DefaultSplitArrays.map(Bytes.concat(_, binBytes))
        prefixes.flatMap { prefix =>
          zs.map { case (lo, hi) =>
            val start = Bytes.concat(prefix, lo)
            val end = Bytes.concat(prefix, hi)
            new Scan(start, end).addFamily(HBaseFeatureIndex.DataColumnFamily) // TODO following prefix?
          }
        }
      }

      ranges.toSeq
    }

    val z3table = ops.connection.getTable(getTableName(sft))
    val clientFilter = filter.filter.map(f => (sf: SimpleFeature) => f.evaluate(sf))

    ScanPlan(filter, z3table, ranges, entriesToFeatures(sft, sft), clientFilter)
  }
}
