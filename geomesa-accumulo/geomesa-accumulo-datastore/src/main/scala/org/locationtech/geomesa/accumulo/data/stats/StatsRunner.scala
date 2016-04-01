/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.stats

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, Executors, Future, TimeUnit}

import org.joda.time.{DateTime, DateTimeUtils, DateTimeZone}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.util.DistributedLocking
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Update stats for a data store in a background thread. Acquires a distributes lock to ensure
  * that work isn't being duplicated.
  *
  * @param ds data store to collect stats for
  */
class StatsRunner(ds: AccumuloDataStore) extends Runnable with DistributedLocking with Closeable {

  override val connector = ds.connector

  private val es = Executors.newSingleThreadScheduledExecutor()
  private val scheduled = new AtomicBoolean(false)
  private val shutdown  = new AtomicBoolean(false)

  /**
    * Runs updates asynchronously. Will continue scheduling itself until 'close' is called.
    *
    * @param initialDelay initial delay, in minutes
    */
  def scheduleRepeating(initialDelay: Int = 0): Unit = {
    scheduled.set(true)
    if (initialDelay > 0) {
      es.schedule(this, initialDelay, TimeUnit.MINUTES)
    } else {
      es.submit(this)
    }
  }

  /**
    * Submits a stat run for the given sft
    *
    * @param sft simple feature type
    * @param delay delay, in minutes before executing
    * @return
    */
  def submit(sft: SimpleFeatureType, delay: Int = 0): Future[DateTime] = {
    val runner = new StatRunner(ds, sft)
    if (delay > 0) {
      es.schedule(runner, delay, TimeUnit.MINUTES)
    } else {
      es.submit(runner)
    }
  }

  /**
    * Checks for the last time stats were run, and runs if needed.
    * Updates metadata accordingly.
    */
  override def run(): Unit = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    // convert to iterator so we check shutdown before each update
    val sfts = ds.getTypeNames.map(ds.getSchema).iterator.filter(_ => !shutdown.get())
    // try to get an exclusive lock on the sft - if not, don't wait just move along
    val lockTimeout = Some(1000L)
    // force execution of iterator
    val minUpdate = sfts.map(new StatRunner(ds, _, lockTimeout).call()).map(_.getMillis).minOption
    // wait at least one minute before running again
    val minRun = 60000L
    val nextRun = minUpdate.map(_ - DateTimeUtils.currentTimeMillis()).filter(_ > minRun).getOrElse(minRun)

    if (scheduled.get() && !shutdown.get()) {
      es.schedule(this, nextRun, TimeUnit.MILLISECONDS)
    }
  }

  override def close(): Unit = {
    shutdown.getAndSet(true)
    es.shutdownNow()
  }
}

class StatRunner(ds: AccumuloDataStore, sft: SimpleFeatureType, lockTimeout: Option[Long] = None) extends
    Callable[DateTime] with DistributedLocking {

  override val connector = ds.connector

  /**
    * Runs stats for the simple feature type
    *
    * @return time of the next scheduled update
    */
  override def call(): DateTime = {
    val updateInterval = getUpdateInterval
    val unsafeUpdate = getLastUpdate.plusMinutes(updateInterval)

    if (unsafeUpdate.isAfterNow) {
      unsafeUpdate
    } else {
      lockTimeout.map(acquireLock(lockKey, _)).getOrElse(Some(acquireLock(lockKey))).map { lock =>
        try {
          // reload next update now that we have the lock
          val nextUpdate = getLastUpdate.plusMinutes(updateInterval)
          if (nextUpdate.isAfterNow) {
            nextUpdate
          } else {
            // run the update
            ds.stats.runStats(sft)
            DateTime.now(DateTimeZone.UTC).plusMinutes(updateInterval)
          }
        } finally {
          lock.release()
        }
      }.getOrElse(DateTime.now(DateTimeZone.UTC).plusMinutes(5)) // defaul to check again in 5 minutes
    }
  }

  /**
    * Reads the time of the last update
    *
    * @return last update
    */
  private def getLastUpdate: DateTime = {
    ds.metadata.readString(sft.getTypeName, STATS_GENERATION_KEY, cache = false) match {
      case Some(dt) => GeoToolsDateFormat.parseDateTime(dt)
      case None     => new DateTime(0, DateTimeZone.UTC)
    }
  }

  /**
    * Reads the update interval.
    *
    * Note: will write the default update interval if the data doesn't exist
    *
    * @return update interval, in minutes
    */
  private def getUpdateInterval: Int = {
    val defaultUpdateInterval = 1440 // in minutes - 1440 is one day
    ds.metadata.readString(sft.getTypeName, STATS_INTERVAL_KEY, cache = false) match {
      case Some(dt) => dt.toInt
      case None =>
        // write the default so that it's there if anyone wants to modify it
        ds.metadata.insert(sft.getTypeName, STATS_INTERVAL_KEY, defaultUpdateInterval.toString)
        defaultUpdateInterval
    }
  }

  private def lockKey: String = {
    val ca = GeoMesaTable.hexEncodeNonAlphaNumeric(ds.catalogTable)
    val tn = GeoMesaTable.hexEncodeNonAlphaNumeric(sft.getTypeName)
    s"/org.locationtech.geomesa/accumulo/stats/$ca/$tn"
  }
}