/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.hbase.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.HBaseFilterStrategy
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

object HBaseQueryPlan extends LazyLogging {

  /**
    * Creates a scanner based on a query plan
    */
  private def getScanner(queryPlan: HBaseQueryPlan): CloseableIterator[Result] = {
    try {
      queryPlan match {
        case qp: EmptyPlan =>
          CloseableIterator.empty

        case qp: ScanPlan =>
          val scanners = qp.ranges.map(qp.table.getScanner)
          CloseableIterator(scanners.flatMap(result).iterator, scanners.foreach(_.close))

        case qp: JoinPlan => ???
//          val primary =
//          val primary = if (qp.ranges.length == 1) {
//            val scanner = acc.getScanner(qp.table)
//            configureScanner(scanner, qp)
//            scanner
//          } else {
//            val batchScanner = acc.getBatchScanner(qp.table, qp.numThreads)
//            configureBatchScanner(batchScanner, qp)
//            batchScanner
//          }
//          val jqp = qp.joinQuery
//          val secondary = acc.getBatchScanner(jqp.table, jqp.numThreads)
//          configureBatchScanner(secondary, jqp)
//
//          val bms = new BatchMultiScanner(primary, secondary, qp.joinFunction)
//          SelfClosingIterator(bms.iterator, () => bms.close())
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error in creating scanner: $e", e)
        // since GeoTools would eat the error and return no records anyway,
        // there's no harm in returning an empty iterator.
        CloseableIterator.empty
    }
  }

  private def result(scanner: ResultScanner): SelfClosingIterator[Result] = {
    import scala.collection.JavaConversions._
    SelfClosingIterator(scanner.toList.iterator, scanner.close())
  }
}

sealed trait HBaseQueryPlan {
  def filter: HBaseFilterStrategy
  def table: Table
  def ranges: Seq[Scan]
  def kvsToFeatures: (Result) => SimpleFeature
  def clientSideFilter: Option[(SimpleFeature) => Boolean]

  def join: Option[((Result) => Scan, HBaseQueryPlan)] = None

  def execute(): CloseableIterator[SimpleFeature] =
    SelfClosingIterator(HBaseQueryPlan.getScanner(this)).map(kvsToFeatures)
}

// plan that will not actually scan anything
case class EmptyPlan(filter: HBaseFilterStrategy) extends HBaseQueryPlan {

  override val table: Table = null
  override val ranges: Seq[Scan] = Seq.empty
  override val kvsToFeatures: (Result) => SimpleFeature = (_) => null
  override val clientSideFilter: Option[(SimpleFeature) => Boolean] = None
}

// single scan plan
case class ScanPlan(filter: HBaseFilterStrategy,
                    table: Table,
                    ranges: Seq[Scan],
                    kvsToFeatures: (Result) => SimpleFeature,
                    clientSideFilter: Option[(SimpleFeature) => Boolean]) extends HBaseQueryPlan

// join on multiple tables - requires multiple scans
case class JoinPlan(filter: HBaseFilterStrategy,
                    table: Table,
                    ranges: Seq[Scan],
                    joinFunction: (Result) => Scan,
                    joinQuery: ScanPlan) extends HBaseQueryPlan {
  override val kvsToFeatures: (Result) => SimpleFeature = joinQuery.kvsToFeatures
  override val join: Option[((Result) => Scan, HBaseQueryPlan)] = Some((joinFunction, joinQuery))
  override val clientSideFilter: Option[(SimpleFeature) => Boolean] = joinQuery.clientSideFilter
}
