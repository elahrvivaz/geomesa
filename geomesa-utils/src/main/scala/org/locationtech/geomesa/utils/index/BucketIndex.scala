/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Envelope
import scala.collection.mutable

class BucketIndex[T](xBuckets: Int = 360,
                     yBuckets: Int = 180,
                     extents: Envelope = new Envelope(-180.0, 180.0, -90.0, 90.0)) extends SpatialIndex[T] {

  // create the buckets upfront to avoid having to synchronize the whole array
  val buckets = Array.fill(xBuckets, yBuckets)(mutable.HashSet.empty[T])
  private val xOffset = (extents.getMaxX - extents.getMinX) / 2.0
  private val yOffset = (extents.getMaxY - extents.getMinY) / 2.0

  override def insert(envelope: Envelope, item: T): Unit = {
    val (i, j) = getBucket(envelope)
    val bucket = buckets(i)(j)
    bucket.synchronized(bucket.add(item))
  }

  override def remove(envelope: Envelope, item: T): Boolean = {
    val (i, j) = getBucket(envelope)
    val bucket = buckets(i)(j)
    bucket.synchronized(bucket.remove(item))
  }

  override def query(envelope: Envelope): Iterator[T] = {
    val mini = Math.floor(envelope.getMinX + xOffset).toInt % xBuckets
    val maxi = (Math.floor(envelope.getMaxX + xOffset).toInt % xBuckets) - 1
    val minj = Math.floor(envelope.getMinY + xOffset).toInt % xBuckets
    val maxj = (Math.floor(envelope.getMaxY + xOffset).toInt % xBuckets) - 1
    new Iterator[T]() {
      var i = mini
      var j = minj - 1
      var iter = Iterator.empty.asInstanceOf[Iterator[T]]
      override def hasNext = {
        while (!iter.hasNext && (i < maxi || j < maxj)) {
          if (j < maxj) {
            j += 1
          } else {
            j = minj
            i += 1
          }
          val bucket = buckets(i)(j)
          iter = bucket.synchronized(bucket.toSeq).iterator
        }
        iter.hasNext
      }

      override def next() = iter.next()
    }
  }

  private def getBucket(envelope: Envelope): (Int, Int) = {
    val i = Math.floor(((envelope.getMinX + envelope.getMaxX) / 2.0) + xOffset).toInt % xBuckets
    val j = Math.round(((envelope.getMinY + envelope.getMaxY) / 2.0) + yOffset).toInt % yBuckets
    (i, j)
  }
}
