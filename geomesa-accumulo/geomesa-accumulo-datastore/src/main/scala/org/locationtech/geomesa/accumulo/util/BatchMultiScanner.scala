/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.Map.Entry
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.collect.Queues
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.ScannerBase
import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator
import org.locationtech.geomesa.accumulo.index.QueryPlanners.JoinFunction
import org.locationtech.geomesa.accumulo.index.{BatchScanPlan, Strategy}

import scala.collection.JavaConversions._

class BatchMultiScanner(acc: AccumuloConnectorCreator,
                        in: ScannerBase,
                        join: BatchScanPlan,
                        joinFunction: JoinFunction,
                        numThreads: Int = 10,
                        batchSize: Int = 32768)
  extends Iterable[java.util.Map.Entry[Key, Value]] with AutoCloseable with LazyLogging {

  require(batchSize > 0, f"Illegal batchSize ($batchSize%d). Value must be > 0")
  require(numThreads > 0, f"Illegal numThreads ($numThreads%d). Value must be > 0")
  logger.trace(f"Creating BatchMultiScanner with batchSize $batchSize%d")

  val executor = Executors.newFixedThreadPool(numThreads)
  val monitoringExecutor = Executors.newSingleThreadExecutor()

  val inQ  = Queues.newLinkedBlockingQueue[Entry[Key, Value]](batchSize)
  val outQ = Queues.newArrayBlockingQueue[Entry[Key, Value]](batchSize)

  val inDone  = new AtomicBoolean(false)
  val outDone = new AtomicBoolean(false)

  executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        in.iterator().foreach(inQ.put)
      } finally {
        inDone.set(true)
      }
    }
  })

  executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        while (!inDone.get || inQ.size() > 0) {
          val entry = inQ.poll(5, TimeUnit.MILLISECONDS)
          if (entry != null) {
            val entries = new collection.mutable.ListBuffer[Entry[Key, Value]]()
            inQ.drainTo(entries)
            executor.submit(new Runnable {
              override def run(): Unit = {
                val ranges = (List(entry) ++ entries).map(joinFunction)
                val scanner = acc.getBatchScanner(join.table, join.numThreads)
                Strategy.configureBatchScanner(scanner, join.copy(ranges = ranges))
                scanner.iterator().foreach(outQ.put)
                scanner.close()
              }
            })
          }
        }
        executor.shutdown()
      } catch {
        case _: InterruptedException =>
      }
    }
  })

  monitoringExecutor.submit(new Runnable {
    override def run(): Unit = {
      try {
        while (!executor.isTerminated) {
          executor.awaitTermination(60, TimeUnit.SECONDS)
        }
      } catch {
        case _: InterruptedException =>
      } finally {
        outDone.set(true)
      }
      monitoringExecutor.shutdown()
    }
  })

  override def close() {
    if (!executor.isShutdown) {
      executor.shutdownNow()
    }
    if (!monitoringExecutor.isShutdown) {
      monitoringExecutor.shutdownNow()
    }
    in.close()
  }

  override def iterator: Iterator[Entry[Key, Value]] = new Iterator[Entry[Key, Value]] {

    var prefetch: Entry[Key, Value] = null

    def prefetchIfNull() = {
      // loop while we might have another and we haven't set prefetch
      while (prefetch == null && (!outDone.get || outQ.size > 0)) {
        prefetch = outQ.poll(5, TimeUnit.MILLISECONDS)
      }
    }

    // must attempt a prefetch since we don't know whether or not the outQ
    // will actually be filled with an item (filters may not match and the
    // in scanner may never return a range)
    override def hasNext(): Boolean = {
      prefetchIfNull()
      prefetch != null
    }

    override def next(): Entry[Key, Value] = {
      prefetchIfNull()
      val ret = prefetch
      prefetch = null
      ret
    }
  }
}
