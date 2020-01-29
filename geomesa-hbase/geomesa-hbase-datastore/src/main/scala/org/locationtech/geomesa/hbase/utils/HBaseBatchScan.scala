/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.util.concurrent._

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.utils.collection.CloseableIterator

private class HBaseBatchScan(table: Table, ranges: Seq[Scan], threads: Int, buffer: Int)
    extends AbstractBatchScan[Scan, Result](ranges, threads, buffer, HBaseBatchScan.Sentinel) {

  override protected def scan(range: Scan, out: BlockingQueue[Result]): Unit = {
    val scan = table.getScanner(range)
    try {
      var result = scan.next()
      while (result != null) {
        out.put(result)
        result = scan.next()
      }
    } finally {
      scan.close()
    }
  }

  override def close(): Unit = {
    super.close()
    table.close()
  }
}

object HBaseBatchScan {

  private val Sentinel = new Result
  private val BufferSize = HBaseSystemProperties.ScanBufferSize.toInt.get

  /**
   * Creates a batch scan with parallelism across the given scans
   *
   * @param connection connection
   * @param table table to scan
   * @param ranges ranges
   * @param threads number of concurrently running scans
   * @return
   */
  def apply(connection: Connection, table: TableName, ranges: Seq[Scan], threads: Int): CloseableIterator[Result] =
    // passing in a null thread pool will cause the table to create and manage its own pool
    new HBaseBatchScan(connection.getTable(table, null), ranges, threads, BufferSize).start()
}
