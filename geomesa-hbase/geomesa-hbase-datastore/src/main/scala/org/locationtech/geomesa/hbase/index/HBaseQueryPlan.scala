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
import org.locationtech.geomesa.hbase.index.QueryPlan.{FeatureFunction, JoinFunction}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

object QueryPlan extends LazyLogging {

  type JoinFunction = (Result) => Scan
  type FeatureFunction = (Result) => SimpleFeature

  private def result(scanner: ResultScanner): SelfClosingIterator[Result] = {
    import scala.collection.JavaConversions._
    SelfClosingIterator(scanner.toList.iterator, scanner.close())
  }

  /**
    * Creates a scanner based on a query plan
    */
  private def getScanner(queryPlan: QueryPlan): CloseableIterator[Result] = {
    try {
      queryPlan match {
        case qp: EmptyPlan =>
          CloseableIterator.empty

        case qp: ScanPlan =>
          val scans = qp.ranges.map { case (l, u) => new Scan(l, u).addFamily(qp.columnFamily) } // TODO calculate this elsewhere?
          val scanners = scans.map(qp.table.getScanner)
          CloseableIterator(scanners.flatMap(result).iterator, scanners.foreach(_.close))

        case qp: JoinPlan => ???
//          val scans = qp.ranges.map { case (l, u) => new Scan(l, u).addFamily(qp.columnFamily) } // TODO calculate this elsewhere?
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
}

sealed trait QueryPlan {
  def filter: HBaseFilterStrategy
  def table: Table
  def ranges: Seq[(Array[Byte], Array[Byte])]
  def columnFamily: Array[Byte]
  def kvsToFeatures: FeatureFunction
  def clientSideFilter: Option[(SimpleFeature) => Boolean]

  def join: Option[(JoinFunction, QueryPlan)] = None

  def execute(): CloseableIterator[SimpleFeature] =
    SelfClosingIterator(QueryPlan.getScanner(this)).map(kvsToFeatures)
}

// plan that will not actually scan anything
case class EmptyPlan(filter: HBaseFilterStrategy) extends QueryPlan {
  override val table: Table = null
  override val ranges: Seq[(Array[Byte], Array[Byte])] = Seq.empty
  override val columnFamily: Array[Byte] = Array.empty
  override val kvsToFeatures: FeatureFunction = (_) => null
  override val clientSideFilter: Option[(SimpleFeature) => Boolean] = None
}

// single scan plan
case class ScanPlan(filter: HBaseFilterStrategy,
                    table: Table,
                    ranges: Seq[(Array[Byte], Array[Byte])],
                    columnFamily: Array[Byte],
                    kvsToFeatures: FeatureFunction,
                    clientSideFilter: Option[(SimpleFeature) => Boolean]) extends QueryPlan

// join on multiple tables - requires multiple scans
case class JoinPlan(filter: HBaseFilterStrategy,
                    table: Table,
                    ranges: Seq[(Array[Byte], Array[Byte])],
                    columnFamily: Array[Byte],
                    joinFunction: JoinFunction,
                    joinQuery: ScanPlan) extends QueryPlan {
  override val kvsToFeatures: FeatureFunction = joinQuery.kvsToFeatures
  override val join: Option[(JoinFunction, QueryPlan)] = Some((joinFunction, joinQuery))
  override val clientSideFilter: Option[(SimpleFeature) => Boolean] = joinQuery.clientSideFilter
}
