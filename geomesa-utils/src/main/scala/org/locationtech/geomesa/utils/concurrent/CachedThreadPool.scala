/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.google.common.util.concurrent.MoreExecutors

/**
 * Executor service that will grow up to the number of threads specified.
 *
 * Assertion: core java executors offer 3 features that we want, but you can only use two of them at once:
 *   1. Add threads as needed but re-use idle threads if available
 *   2. Limit the total number of threads available
 *   3. Accept and queue tasks once the thread limit is reached
 *
 * This executor allows us to do all three. It is backed by an unlimited, cached thread pool that will handle
 * re-use and expiration of threads, while it tracks the number of threads used. It is fairly lightweight, and
 * can be instantiated for a given task and then shutdown after use.
 *
 * @param maxThreads max threads to use at once
 */
class CachedThreadPool(maxThreads: Int) extends AbstractExecutorService {

  @volatile
  private var available = maxThreads
  private val queue = new java.util.LinkedList[TrackableFutureTask[_]]()
  private val tasks = new java.util.HashSet[Future[_]]()
  private val stopped = new AtomicBoolean(false)
  private val lock = new ReentrantLock()

  override def shutdown(): Unit = stopped.set(true)

  override def shutdownNow(): java.util.List[Runnable] = {
    stopped.set(true)
    lock.lock()
    try {
      val waiting = new java.util.ArrayList[Runnable](queue)
      queue.clear()
      // copy the running tasks to prevent concurrent modification errors in the synchronous cancel call
      val running = new java.util.ArrayList[Future[_]](tasks).iterator()
      while (running.hasNext) {
        running.next.cancel(true)
      }
      waiting
    } finally {
      lock.unlock()
    }
  }

  override def isShutdown: Boolean = stopped.get

  override def isTerminated: Boolean = stopped.get && available == maxThreads // should be safe to read a volatile primitive

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
    // TODO GEOMESA-2795 could probably make a wait instead of a poll
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
      case c => newTaskFor(c, null)
    }
    runOrQueueTask(task)
  }

  private def runOrQueueTask(task: TrackableFutureTask[_]): Unit = {
    lock.lock()
    try {
      if (available > 0) {
        available -= 1
        tasks.add(task)
        // note that we could fairly easily create a global thread limit by backing this with a different pool
        CachedThreadPool.pool.execute(task)
      } else {
        queue.offer(task) // unbounded queue so should always succeed
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
        available += 1
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

  /**
   * Execute a single command in a potentially cached thread
   *
   * @param command command
   */
  def execute(command: Runnable): Unit = pool.execute(command)

  /**
   * Submit a single command to run in a potentially cached thread
   *
   * @param command command
   * @return
   */
  def submit(command: Runnable): Future[_] = pool.submit(command)
}
