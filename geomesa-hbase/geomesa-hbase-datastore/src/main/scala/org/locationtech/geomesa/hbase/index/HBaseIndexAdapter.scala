/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase.coprocessor.aggregators._
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.data.{EmptyPlan, HBaseDataStore, HBaseFeature, HBaseQueryPlan}
import org.locationtech.geomesa.hbase.filters.JSimpleFeatureFilter
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.hbase.{HBaseFeatureIndexType, HBaseFilterStrategyType, HBaseQueryPlanType}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.index.iterators.{ArrowBatchScan, ArrowScan}
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait HBaseIndexAdapter extends HBaseFeatureIndexType
    with IndexAdapter[HBaseDataStore, HBaseFeature, Mutation, Query, ScanConfig] with ClientSideFiltering[Result] {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}

  override def rowAndValue(result: Result): RowAndValue = {
    val cell = result.rawCells()(0)
    RowAndValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }

  override protected def createInsert(row: Array[Byte], feature: HBaseFeature): Mutation = {
    val put = new Put(row).addImmutable(feature.fullValue.cf, feature.fullValue.cq, feature.fullValue.value)
    feature.fullValue.vis.foreach(put.setCellVisibility)
    put
  }

  override protected def createDelete(row: Array[Byte], feature: HBaseFeature): Mutation = {
    val del = new Delete(row).addFamily(feature.fullValue.cf)
    feature.fullValue.vis.foreach(del.setCellVisibility)
    del
  }

  override protected def range(start: Array[Byte], end: Array[Byte]): Query =
    new Scan(start, end).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def rangeExact(row: Array[Byte]): Query =
    new Get(row).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: HBaseFilterStrategyType,
                                  config: ScanConfig): HBaseQueryPlanType = {
    if (config.ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val ScanConfig(ranges, hbaseFilters, coprocessor, toFeatures) = config
      buildPlatformScanPlan(ds, sft, filter, ranges, table, hbaseFilters, coprocessor, toFeatures)
    }
  }

  protected def scanConfig(sft: SimpleFeatureType,
                           ds: HBaseDataStore,
                           filter: HBaseFilterStrategyType,
                           ranges: Seq[Query],
                           ecql: Option[Filter],
                           hints: Hints): ScanConfig = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val transform: Option[(String, SimpleFeatureType)] = hints.getTransform
    val dedupe = hasDuplicates(sft, filter.primary)

    if (!ds.config.remoteFilter) {
      // everything is done client side
      ScanConfig(ranges, Seq.empty, None, resultsToFeatures(sft, ecql, transform))
    } else {

      val (remoteTdefArg, returnSchema) = transform.getOrElse(("", sft))

      // TODO not actually used for coprocessors
      val toFeatures = resultsToFeatures(returnSchema, None, None)

      val coprocessorConfig = if (hints.isDensityQuery) {
        val options = HBaseDensityAggregator.configure(sft, this, ecql, hints)
        Some(CoprocessorConfig(options, HBaseDensityAggregator.bytesToFeatures))
      } else if (hints.isArrowQuery) {
        if (Option(hints.get(QueryHints.ARROW_SINGLE_PASS)).exists(_.asInstanceOf[Boolean])) {
          val dictionaries = hints.getArrowDictionaryFields
          val options = ArrowAggregator.configure(sft, this, ecql, dictionaries, hints)
          val reduce = ArrowScan.reduceFeatures(hints.getTransformSchema.getOrElse(sft), dictionaries, hints)
          Some(CoprocessorConfig(options, ArrowAggregator.bytesToFeatures, reduce))
        } else {
          val dictionaryFields = hints.getArrowDictionaryFields
          val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
          if (hints.getArrowSort.isDefined || hints.isArrowComputeDictionaries ||
              dictionaryFields.forall(providedDictionaries.contains)) {
            val dictionaries = ArrowBatchScan.createDictionaries(ds.stats, sft, filter.filter, dictionaryFields,
              providedDictionaries, hints.isArrowCachedDictionaries)
            val options = ArrowBatchAggregator.configure(sft, this, ecql, dictionaries, hints)
            val reduce = ArrowBatchScan.reduceFeatures(hints.getTransformSchema.getOrElse(sft), hints, dictionaries)
            Some(CoprocessorConfig(options, ArrowBatchAggregator.bytesToFeatures, reduce))
          } else {
            val options = ArrowFileAggregator.configure(sft, this, ecql, dictionaryFields, hints)
            Some(CoprocessorConfig(options, ArrowFileAggregator.bytesToFeatures))
          }
        }
      } else if (hints.isStatsQuery) {
        val statsOptions = HBaseStatsAggregator.configure(sft, filter.index, ecql, hints)
        Some(CoprocessorConfig(statsOptions, HBaseStatsAggregator.bytesToFeatures, KryoLazyStatsUtils.reduceFeatures(returnSchema, hints)))
      } else if (hints.isBinQuery) {
        val options = HBaseBinAggregator.configure(sft, filter.index, ecql, hints)
        Some(CoprocessorConfig(options, HBaseBinAggregator.bytesToFeatures))
      } else {
        None
      }

      // if there is a coprocessorConfig it handles filter/transform
      val filters = if (coprocessorConfig.isDefined || (ecql.isEmpty && transform.isEmpty)) {
        Seq.empty
      } else {
        val remoteCQLFilter: Filter = ecql.getOrElse(Filter.INCLUDE)
        val encodedSft = SimpleFeatureTypes.encodeType(returnSchema)
        val filter = new JSimpleFeatureFilter(sft, remoteCQLFilter, remoteTdefArg, encodedSft)
        Seq((JSimpleFeatureFilter.Priority, filter))
      }

      ScanConfig(ranges, filters, coprocessorConfig, toFeatures)
    }
  }

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false

  protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                      sft: SimpleFeatureType,
                                      filter: HBaseFilterStrategyType,
                                      ranges: Seq[Query],
                                      table: TableName,
                                      hbaseFilters: Seq[(Int, HFilter)],
                                      coprocessor: Option[CoprocessorConfig],
                                      toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan
}

object HBaseIndexAdapter {
  case class ScanConfig(ranges: Seq[Query],
                        filters: Seq[(Int, HFilter)],
                        coprocessor: Option[CoprocessorConfig],
                        entriesToFeatures: Iterator[Result] => Iterator[SimpleFeature])
}