/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Lock, ReentrantLock}

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.retry.ExponentialBackoffRetry

trait DistributedLocking {

  def connector: Connector

  /**
   * * Gets and acquires a distributed lock based on the path.
   * Make sure that you 'release' the lock in a finally block.
   *
   * @param key key to lock on - equivalent to a path in zookeeper
   * @param timeOut how long to wait for the lock before throwing an exception, in millis
   * @return the lock
   */
  protected def lock(key: String, timeOut: Long = 10000): Option[Releasable] = {
    if (connector.isInstanceOf[MockConnector]) {
      import DistributedLocking.mockLocks
      val lock = mockLocks.synchronized(mockLocks.getOrElseUpdate(key, new ReentrantLock()))
      if (lock.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
        Some(new Releasable { override def release(): Unit = lock.unlock() })
      } else {
        None
      }
    } else {
      val backOff = new ExponentialBackoffRetry(1000, 3)
      val client = CuratorFrameworkFactory.newClient(connector.getInstance().getZooKeepers, backOff)
      client.start()
      try {
        val lockPath = if (key.startsWith("/")) key else s"/$key"
        val lock = new InterProcessSemaphoreMutex(client, lockPath)
        if (lock.acquire(timeOut, TimeUnit.MILLISECONDS)) {
          // delegate lock that will close the curator client upon release
          Some(new Releasable { override def release(): Unit = try { lock.release() } finally { client.close() } })
        } else {
          None
        }
      } catch {
        case e: Exception => client.close(); throw e
      }
    }
  }
}

object DistributedLocking {
  private lazy val mockLocks = scala.collection.mutable.Map.empty[String, Lock]
}

trait Releasable {
  def release(): Unit
}
