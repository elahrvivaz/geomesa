/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, RecordTable}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanners.JoinFunction
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}
import org.opengis.filter.{Filter, PropertyIsEqualTo, PropertyIsLike, _}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class AttributeIdxStrategy(val filter: QueryFilter) extends Strategy with Logging {

  import org.locationtech.geomesa.accumulo.index.AttributeIdxStrategy._

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  override def getQueryPlans(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    // pull out any dates from the filter to help narrow down the attribute ranges
    val dates = {
      val (dateFilters, _) = filter.secondary.map {
        case a: And => partitionPrimaryTemporals(a.getChildren, queryPlanner.sft)
        case f      => partitionPrimaryTemporals(Seq(f), queryPlanner.sft)
      }.getOrElse((Seq.empty, Seq.empty))
      val interval = extractTemporal(queryPlanner.sft.getDtgField)(dateFilters)
      if (interval == everywhen) None else Some((interval.getStartMillis, interval.getEndMillis))
    }

    val propsAndRanges = filter.primary.map(getPropertyAndRange(queryPlanner.sft, _, dates))
    val attributeSftIndex = propsAndRanges.head._1
    val ranges = propsAndRanges.map(_._2)
    // ensure we only have 1 prop we're working on
    assert(propsAndRanges.forall(_._1 == attributeSftIndex))

    val descriptor = sft.getDescriptor(attributeSftIndex)
    val transform = hints.getTransformSchema
    val attributeName = descriptor.getLocalName
    val hasDupes = descriptor.isMultiValued

    val attributeTable = acc.getAttributeTable(sft)
    val attributeThreads = acc.getSuggestedAttributeThreads(sft)
    val priority = FILTERING_ITER_PRIORITY

    // query against the attribute table
    val singleTableScanPlan: ScanPlanFn = (schema, filter, transform) => {
      val iterators = if (filter.isDefined || transform.isDefined) {
        Seq(KryoLazyFilterTransformIterator.configure(schema, filter, transform, priority))
      } else {
        Seq.empty
      }
      val kvsToFeatures = queryPlanner.kvsToFeatures(transform.map(_._2).getOrElse(schema))
      BatchScanPlan(attributeTable, ranges, iterators, Seq.empty, kvsToFeatures, attributeThreads, hasDupes)
    }

    if (hints.isBinQuery) {
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        // can apply the bin aggregating iterator directly to the sft
        val iters = Seq(BinAggregatingIterator.configureDynamic(sft, hints, filter.secondary, priority))
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        Seq(BatchScanPlan(attributeTable, ranges, iters, Seq.empty, kvsToFeatures, attributeThreads, hasDupes))
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
            hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iters = Seq(BinAggregatingIterator.configureDynamic(indexSft, hints, filter.secondary, priority))
          val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
          Seq(BatchScanPlan(attributeTable, ranges, iters, Seq.empty, kvsToFeatures, attributeThreads, hasDupes))
        } else {
          // have to do a join against the record table
          joinQuery(sft, hints, queryPlanner, hasDupes, singleTableScanPlan)
        }
      }
    } else if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
      // we have a fully encoded value - can satisfy any query against it
      Seq(singleTableScanPlan(sft, filter.secondary, hints.getTransform))
    } else if (IteratorTrigger.canUseIndexValues(sft, filter.secondary, transform)) {
      // we can use the index value
      // transform has to be non-empty to get here
      Seq(singleTableScanPlan(IndexValueEncoder.getIndexSft(sft), filter.secondary, hints.getTransform))
    } else {
      // have to do a join against the record table
      joinQuery(sft, hints, queryPlanner, hasDupes, singleTableScanPlan)
    }
  }

  def joinQuery(sft: SimpleFeatureType,
                hints: Hints,
                queryPlanner: QueryPlanner,
                hasDupes: Boolean,
                attributePlan: ScanPlanFn): Seq[JoinPlan] = {
    // we have to do a join against the record table
    // break out the st filter to evaluate against the attribute table
    val (stFilter, ecqlFilter) = filter.secondary.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, sft)
      val st = andOption(geomFilters ++ temporalFilters)
      val ecql = andOption(nonSTFilters)
      (st, ecql)
    }.getOrElse((None, None))

    // the scan against the attribute table
    val attributeScan = attributePlan(IndexValueEncoder.getIndexSft(sft), stFilter, None)

    // apply any secondary filters or transforms against the record table
    val recordIterators = if (ecqlFilter.isDefined || hints.getTransformSchema.isDefined) {
      Seq(configureRecordTableIterator(sft, queryPlanner.featureEncoding, ecqlFilter, hints))
    } else {
      Seq.empty
    }
    val kvsToFeatures = if (hints.isBinQuery) {
      // TODO GEOMESA-822 we can use the aggregating iterator if the features are kryo encoded
      BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, hints, queryPlanner.featureEncoding)
    } else {
      queryPlanner.defaultKVsToFeatures(hints)
    }

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val prefix = sft.getTableSharingPrefix
    val getIdFromRow = AttributeTable.getIdFromRow(sft)
    val joinFunction: JoinFunction =
      (kv) => new AccRange(RecordTable.getRowKey(prefix, getIdFromRow(kv.getKey.getRow.getBytes)))

    val recordTable = queryPlanner.acc.getRecordTable(sft)
    val recordRanges = Seq(new AccRange()) // this will get overwritten in the join method
    val recordThreads = queryPlanner.acc.getSuggestedRecordThreads(sft)
    val joinQuery = BatchScanPlan(recordTable, recordRanges, recordIterators, Seq.empty,
      kvsToFeatures, recordThreads, hasDupes)

    Seq(JoinPlan(attributeScan.table, attributeScan.ranges, attributeScan.iterators,
      attributeScan.columnFamilies, recordThreads, hasDupes, joinFunction, joinQuery))
  }
}

object AttributeIdxStrategy extends StrategyProvider {

  val FILTERING_ITER_PRIORITY = 25
  type ScanPlanFn = (SimpleFeatureType, Option[Filter], Option[(String, SimpleFeatureType)]) => BatchScanPlan

  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) = {
    val cost = filter.primary.flatMap(getAttributeProperty).map { p =>
      val descriptor = sft.getDescriptor(p.name)
      val multiplier = if (descriptor.getIndexCoverage() == IndexCoverage.JOIN) 2 else 1
      hints.cardinality(descriptor) match {
        case Cardinality.HIGH    => 1 * multiplier
        case Cardinality.UNKNOWN => 999 * multiplier
        case Cardinality.LOW     => Int.MaxValue
      }
    }.sum
    if (cost == 0) Int.MaxValue else cost // cost == 0 if somehow the filters don't match anything
  }

  /**
   * Gets a row key that can used as a range for an attribute query.
   * The attribute index encodes the type of the attribute as part of the row. This checks for
   * query literals that don't match the expected type and tries to convert them.
   */
  def getEncodedAttrIdxRow(sft: SimpleFeatureType, prop: Int, value: Any, time: Option[Long]): Text = {
    val descriptor = sft.getDescriptor(prop)
    // the class type as defined in the SFT
    val expectedBinding = descriptor.getType.getBinding
    // the class type of the literal pulled from the query
    val actualBinding = value.getClass
    val typedValue = if (expectedBinding == actualBinding) {
      value
    } else if (descriptor.isCollection) {
      // we need to encode with the collection type
      descriptor.getCollectionType() match {
        case Some(collectionType) if collectionType == actualBinding => Seq(value).asJava
        case Some(collectionType) if collectionType != actualBinding =>
          Seq(AttributeTable.convertType(value, actualBinding, collectionType)).asJava
      }
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Map.empty.asJava
    } else {
      // type mismatch, encoding won't work b/c value is wrong class
      // try to convert to the appropriate class
      AttributeTable.convertType(value, actualBinding, expectedBinding)
    }

    // if the value resulted in a valid row, use that, otherwise use the prefix row
    val bytes = AttributeTable.getRow(sft, prop, typedValue, time)
        .getOrElse(AttributeTable.getRowPrefix(sft, prop))
    new Text(bytes)
  }

  /**
   * Gets the property name from the filter and a range that covers the filter in the attribute table.
   * Note that if the filter is not a valid attribute filter this method will throw an exception.
   */
  def getPropertyAndRange(sft: SimpleFeatureType,
                          filter: Filter,
                          dates: Option[(Long, Long)]): (Int, AccRange) = {
    filter match {
      case f: PropertyIsBetween =>
        val prop = sft.indexOf(f.getExpression.asInstanceOf[PropertyName].getPropertyName)
        val lower = f.getLowerBoundary.asInstanceOf[Literal].getValue
        val upper = f.getUpperBoundary.asInstanceOf[Literal].getValue
        (prop, inclusiveRange(sft, prop, lower, upper, dates))

      case f: PropertyIsGreaterThan =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, ltRange(sft, idx, prop.literal.getValue, dates.map(_._2)))
        } else {
          (idx, gtRange(sft, idx, prop.literal.getValue, dates.map(_._1)))
        }

      case f: PropertyIsGreaterThanOrEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, lteRange(sft, idx, prop.literal.getValue, dates.map(_._2)))
        } else {
          (idx, gteRange(sft, idx, prop.literal.getValue, dates.map(_._1)))
        }

      case f: PropertyIsLessThan =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, gtRange(sft, idx, prop.literal.getValue, dates.map(_._1)))
        } else {
          (idx, ltRange(sft, idx, prop.literal.getValue, dates.map(_._2)))
        }

      case f: PropertyIsLessThanOrEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, gteRange(sft, idx, prop.literal.getValue, dates.map(_._1)))
        } else {
          (idx, lteRange(sft, idx, prop.literal.getValue, dates.map(_._2)))
        }

      case f: Before =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        val lit = prop.literal.evaluate(null, classOf[Date])
        if (prop.flipped) {
          (idx, gtRange(sft, idx, lit, dates.map(_._1)))
        } else {
          (idx, ltRange(sft, idx, lit, dates.map(_._2)))
        }

      case f: After =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        val lit = prop.literal.evaluate(null, classOf[Date])
        if (prop.flipped) {
          (idx, ltRange(sft, idx, lit, dates.map(_._2)))
        } else {
          (idx, gtRange(sft, idx, lit, dates.map(_._1)))
        }

      case f: During =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        val during = prop.literal.getValue.asInstanceOf[DefaultPeriod]
        val lower = during.getBeginning.getPosition.getDate
        val upper = during.getEnding.getPosition.getDate
        (idx, inclusiveRange(sft, idx, lower, upper, dates))

      case f: PropertyIsEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        (idx, inclusiveRange(sft, idx, prop.literal.getValue, prop.literal.getValue, dates))

      case f: TEquals =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        (idx, inclusiveRange(sft, idx, prop.literal.getValue, prop.literal.getValue, dates))

      case f: PropertyIsLike =>
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val idx = sft.indexOf(prop)
        // Remove the trailing wildcard and create a range prefix
        val literal = f.getLiteral
        val value = if (literal.endsWith(MULTICHAR_WILDCARD)) {
          literal.substring(0, literal.length - MULTICHAR_WILDCARD.length)
        } else {
          literal
        }
        // the row includes a NULL terminator byte - strip that off
        val prefix = new Text(getEncodedAttrIdxRow(sft, idx, value, None).getBytes.dropRight(1))
        (idx, AccRange.prefix(prefix))

      case n: Not =>
        val f = n.getFilter.asInstanceOf[PropertyIsNull] // this should have been verified in getStrategy
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val idx = sft.indexOf(prop)
        (idx, allRange(sft, idx))

      case _ =>
        val msg = s"Unhandled filter type in attribute strategy: ${filter.getClass.getName}"
        throw new RuntimeException(msg)
    }
  }

  // greater than
  private def gtRange(sft: SimpleFeatureType, prop: Int, lit: AnyRef, date: Option[Long]): AccRange = {
    val start = AccRange.followingPrefix(new Text(getEncodedAttrIdxRow(sft, prop, lit, date)))
    val end = upperBound(sft, prop)
    new AccRange(start, true, end, false)
  }

  // greater than or equal to
  private def gteRange(sft: SimpleFeatureType, prop: Int, lit: AnyRef, date: Option[Long]): AccRange = {
    val start = new Text(getEncodedAttrIdxRow(sft, prop, lit, date))
    val end = upperBound(sft, prop)
    new AccRange(start, true, end, false)
  }

  // less than
  private def ltRange(sft: SimpleFeatureType, prop: Int, lit: AnyRef, date: Option[Long]): AccRange = {
    val start = lowerBound(sft, prop)
    val end = new Text(getEncodedAttrIdxRow(sft, prop, lit, date))
    new AccRange(start, false, end, false)
  }

  // less than or equal to
  private def lteRange(sft: SimpleFeatureType, prop: Int, lit: AnyRef, date: Option[Long]): AccRange = {
    val start = lowerBound(sft, prop)
    val end = AccRange.followingPrefix(new Text(getEncodedAttrIdxRow(sft, prop, lit, date)))
    new AccRange(start, false, end, false)
  }

  // [lower, upper]
  private def inclusiveRange(sft: SimpleFeatureType,
                             prop: Int,
                             lower: AnyRef, upper: AnyRef,
                             dates: Option[(Long, Long)]): AccRange = {
    val start = getEncodedAttrIdxRow(sft, prop, lower, dates.map(_._1))
    val end = AccRange.followingPrefix(getEncodedAttrIdxRow(sft, prop, upper, dates.map(_._2)))
    new AccRange(start, true, end, false)
  }

  // equals
  private def eqRange(sft: SimpleFeatureType, prop: Int, lit: AnyRef, dates: Option[(Long, Long)]): AccRange = {
    if (dates.isEmpty) {
      val start = getEncodedAttrIdxRow(sft, prop, lit, None)
      val end = AccRange.followingPrefix(start)
      new AccRange(start, true, end, false)
    } else {
      inclusiveRange(sft, prop, lit, lit, dates)
    }
  }

  // range for all values of the attribute
  private def allRange(sft: SimpleFeatureType, prop: Int): AccRange =
    new AccRange(lowerBound(sft, prop), false, upperBound(sft, prop), false)

  // lower bound for all values of the attribute, exclusive
  private def lowerBound(sft: SimpleFeatureType, prop: Int): Text =
    new Text(AttributeTable.getRowPrefix(sft, prop))

  // upper bound for all values of the attribute, exclusive
  private def upperBound(sft: SimpleFeatureType, prop: Int): Text = {
    val end = new Text(AttributeTable.getRowPrefix(sft, prop))
    AccRange.followingPrefix(end)
  }
}
