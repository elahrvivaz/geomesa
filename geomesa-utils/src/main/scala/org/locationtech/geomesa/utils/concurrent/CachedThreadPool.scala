/*
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.concurrent

import java.util.Collections
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock

import com.google.common.util.concurrent.MoreExecutors

class CachedThreadPool private (maxThreads: Int, delegate: ExecutorService)
    extends AbstractExecutorService {

  private val stopped = new AtomicBoolean(false)
  private val available = new AtomicInteger(maxThreads)
  private val queue = new LinkedBlockingQueue[TrackableFutureTask[_]]()
  private val tasks = Collections.newSetFromMap(new ConcurrentHashMap[Future[_], java.lang.Boolean]())
  private val lock = new ReentrantLock()

  override def shutdown(): Unit = stopped.set(true)

  override def shutdownNow(): java.util.List[Runnable] = {
    stopped.set(true)
    val waiting = new java.util.ArrayList[Runnable]()
    queue.drainTo(waiting)
    val iter = tasks.iterator()
    while (iter.hasNext) {
      iter.next.cancel(true)
    }
    waiting
  }

  override def isShutdown: Boolean = stopped.get

  override def isTerminated: Boolean = stopped.get && available.get == maxThreads

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
    // TODO could probably make a wait instead of a poll
    val end = System.nanoTime() + unit.toNanos(timeout)
    while (!isTerminated && System.nanoTime() < end) {
      Thread.sleep(100)
    }
    isTerminated
  }

  override def execute(command: Runnable): Unit = {
    if (stopped.get) {
      throw new IllegalStateException("Trying to execute a task but executor service is shut down")
    }
    val task = command match {
      case t: TrackableFutureTask[_] => t
      case c => newTaskFor[_](c, null)
    }
    runOrQueueTask(task)
  }

  private def runOrQueueTask(task: TrackableFutureTask[_]): Unit = {
    lock.lock()
    try {
      if (available.getAndDecrement() > 0) {
        tasks.add(task)
        delegate.execute(task)
      } else {
        queue.offer(task) // unbounded queue so should always succeed
        available.incrementAndGet() // re-increment since we didn't successfully grab a spot
      }
    } finally {
      lock.unlock()
    }
  }

  override protected def newTaskFor[T](runnable: Runnable, value: T): TrackableFutureTask[T] =
    new TrackableFutureTask[T](runnable, value)

  class TrackableFutureTask[T](runnable: Runnable, result: T) extends FutureTask[T](runnable, result) {
    override def done(): Unit = {
      lock.lock()
      try {
        tasks.remove(this)
        available.incrementAndGet()
        val next = queue.poll()
        if (next != null) {
          runOrQueueTask(next) // note: this may briefly use more than maxThreads as this thread finishes up
        }
      } finally {
        lock.unlock()
      }
    }
  }
}

object CachedThreadPool {

  // unlimited size but re-uses cached threads
  private val pool =
    MoreExecutors.getExitingExecutorService(Executors.newCachedThreadPool().asInstanceOf[ThreadPoolExecutor])

  def apply(maxThreads: Int): CachedThreadPool = new CachedThreadPool(maxThreads, pool)
}
