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
import org.apache.hadoop.hbase.util.Threads
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging

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
   * @param connection
   * @param table
   * @param ranges
   * @param threads
   * @return
   */
  def apply(connection: Connection, table: TableName, ranges: Seq[Scan], threads: Int): CloseableIterator[Result] =
    new HBaseBatchScan(connection.getTable(table), ranges, threads, BufferSize).start()

  /**
   * Creates a batch scan with parallelism across the associated region servers
   *
   * @param connection
   * @param table
   * @param range
   * @param threads
   * @return
   */
  def apply(connection: Connection, table: TableName, range: Scan, threads: Int): CloseableIterator[Result] =
    new ScanIterator(connection, table, range, threads)

  /**
   * CloseableIterator wrapper for an HBase scan
   *
   * @param connection
   * @param table
   * @param range
   * @param threads
   */
  private class ScanIterator(connection: Connection, table: TableName, range: Scan, threads: Int)
      extends CloseableIterator[Result] {

    private val factory = new ThreadFactory() {
      private val namedFactory: ThreadFactory = Threads.getNamedThreadFactory("geomesa-batch-scan")
      override def newThread(r: Runnable): Thread = {
        val t = namedFactory.newThread(r)
        t.setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER)
        t.setPriority(Thread.NORM_PRIORITY)
        println(s"creating new thread for scan against $table")
        new Exception().printStackTrace()
        t
      }
    }

    private val pool = Executors.newFixedThreadPool(threads, factory)
    private val htable = connection.getTable(table, pool)
    private val scan = htable.getScanner(range)

    private var result: Result = _

    override def hasNext: Boolean = {
      if (result == null) {
        result = scan.next()
      }
      result != null
    }

    override def next(): Result = {
      val n = result
      result = null
      n
    }

    override def close(): Unit = {
      CloseWithLogging(scan, htable)
      pool.shutdownNow()
    }
  }
}
