/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import com.google.common.collect.Lists
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Query, Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter, Filter => HFilter}
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.data.{CoprocessorPlan, HBaseDataStore, HBaseQueryPlan, ScanPlan}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HBasePlatform extends HBaseIndexAdapter {

  override protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                               sft: SimpleFeatureType,
                                               filter: HBaseFilterStrategyType,
                                               ranges: Seq[Query],
                                               table: TableName,
                                               hbaseFilters: Seq[(Int, HFilter)],
                                               coprocessor: Option[CoprocessorConfig],
                                               toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan = {
    coprocessor match {
      case None =>
        // optimize the scans
        val scans = ranges.head match {
          case t: Get  => configureGet(ranges.asInstanceOf[Seq[Get]], hbaseFilters)
          case t: Scan => configureMultiRowRangeFilter(ds, ranges.asInstanceOf[Seq[Scan]], hbaseFilters)
        }
        ScanPlan(filter, table, scans, toFeatures)

      case Some(coprocessorConfig) =>
        // note: coprocessors don't currently handle multiRowRangeFilters, so pass the raw ranges
        CoprocessorPlan(filter, table, ranges.asInstanceOf[Seq[Scan]], hbaseFilters, coprocessorConfig)
    }
  }

  private def configureGet(originalRanges: Seq[Get], hbaseFilters: Seq[(Int, HFilter)]): Seq[Scan] = {
    val filterList = if (hbaseFilters.isEmpty) { None } else {
      Some(new FilterList(hbaseFilters.sortBy(_._1).map(_._2): _*))
    }
    // convert Gets to Scans for Spark SQL compatibility
    originalRanges.map { r =>
      val scan = new Scan(r).setSmall(true)
      filterList.foreach(scan.setFilter)
      scan
    }
  }

  private def configureMultiRowRangeFilter(ds: HBaseDataStore,
                                           originalRanges: Seq[Scan],
                                           hbaseFilters: Seq[(Int, HFilter)]) = {
    import scala.collection.JavaConversions._

    val sortedFilters = hbaseFilters.sortBy(_._1).map(_._2)

    val rowRanges = Lists.newArrayList[RowRange]()
    originalRanges.foreach { r =>
      rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false))
    }
    val sortedRowRanges = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val numRanges = sortedRowRanges.length
    val numThreads = ds.config.queryThreads
    // TODO GEOMESA-1806 parameterize this?
    val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan, math.max(1, math.ceil(numRanges / numThreads * 2).toInt))
    // TODO GEOMESA-1806 align partitions with region boundaries
    val groupedRanges = Lists.partition(sortedRowRanges, rangesPerThread)

    // group scans into batches to achieve some client side parallelism
    val groupedScans = groupedRanges.map { localRanges =>
      // TODO GEOMESA-1806
      // currently, this constructor will call sortAndMerge a second time
      // this is unnecessary as we have already sorted and merged above
      val mrrf = new MultiRowRangeFilter(localRanges)
      // note: mrrf first priority
      val filterList = new FilterList(sortedFilters.+:(mrrf): _*)

      val s = new Scan()
      s.setStartRow(localRanges.head.getStartRow)
      s.setStopRow(localRanges.get(localRanges.length - 1).getStopRow)
      s.setFilter(filterList)
      // TODO GEOMESA-1806 parameterize cache size
      s.setCaching(1000)
      s.setCacheBlocks(true)
      s
    }

    // Apply Visibilities
    groupedScans.foreach(ds.applySecurity)
    groupedScans
  }
}
