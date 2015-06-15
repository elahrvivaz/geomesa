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

class PointQuadtree[T] extends SpatialIndex[T] {

  private val root = QtNode[T](0, 0) // x is lon, y is lat

  override def insert(envelope: Envelope, item: T) = ???

  override def remove(envelope: Envelope, item: T) = ???

  override def query(envelope: Envelope) = ???
}

case class QtNode[T](x: Double, y: Double) {
  val extents = Array(x, x, y, y) // low x, hi x, low y, hi y
  val values  = mutable.HashSet.empty[QtValue[T]]

  var nw: QtNode[T] = null
  var ne: QtNode[T] = null
  var sw: QtNode[T] = null
  var se: QtNode[T] = null

// TODO synchronization
  def insert(value: QtValue[T]): Unit = {
    ensureExtents(value.x, value.y)
    val done = values.synchronized {
      if (nw == null) {
        if (values.size < PointQuadtree.MAX_VALUES_PER_NODE) {
          values.add(value)
        } else {
          subdivide()
          false
        }
      } else {
        false
      }
    }
    if (!done) {
      insertIntoQuadrant(value)
    }
  }

  def find(minx: Double, maxx: Double, miny: Double, maxy: Double): Iterator[T] = {
    val result = values.synchronized {
      if (nw == null) {
        values.filter(value => value.x >= minx && value.x <= maxx && value.y >= miny && value.y <= maxy)
      } else {
        null
      }
    }
    if (result == null) {
      new Iterator[T]() {
        var iter = Iterator.empty
        var count = 0
        override def hasNext = {
          while (!iter.hasNext && count < 4) {

          }
          if (iter.hasNext) {
            true
          } else {

            findNextIter
          }
        }

        private def findNextIter: Boolean = {
          if (count == 0) {

          }
          false
        }
        override def next() = iter.next()
      }
      nw.find(minx, maxx, miny, maxy) ++ ne.find(minx, maxx, miny, maxy) ++ sw.find(minx, maxx, miny, maxy) ++ se.find(minx, maxx, miny, maxy)
    } else {
      result.iterator.map(_.value)
    }
  }

  def remove(value: QtValue[T]): Boolean = {
    if (isInside(value.x, value.y)) {
      val (removed, checkQuadrants) = values.synchronized {
        if (nw == null) {
          (values.remove(value), false)
        } else {
          (false, true)
        }
      }
      if (checkQuadrants) {
        nw.remove(value) || ne.remove(value) || sw.remove(value) || se.remove(value)
      } else {
        removed
      }
    } else {
      false
    }
  }

  private def isInside(x: Double, y: Double): Boolean = extents.synchronized {
    x >= extents(0) && x <= extents(1) && y >= extents(2) && y <= extents(3)
  }

  private def ensureExtents(x: Double, y: Double): Unit = extents.synchronized {
    if (extents(0) > x) {
      extents(0) = x
    } else if (extents(1) < x) {
      extents(1) = x
    }
    if (extents(2) > y) {
      extents(2) = y
    } else if (extents(3) < y) {
      extents(3) = y
    }
  }

  // should only be accessed inside value sync block
  private def subdivide(): Unit = {
    val x2 = (x + 180) / 2
    val y2 = (y + 90) / 2
    val xx2 = (x + 180) + x2
    val yy2 = (y + 90) + y2
    nw = QtNode[T]((x + 180) / 2 - 180, (y + 45) / 2 + 45)
    ne = QtNode[T]((x + 180) / 2 + 180, yy2)
    sw = QtNode[T](x2, y2)
    se = QtNode[T](xx2, y2)
    values.foreach(insertIntoQuadrant)
    values.clear()
  }

  private def insertIntoQuadrant(value: QtValue[T]): Unit = {
    if (value.x < x) {
      if (value.y < y) {
        sw.insert(value)
      } else {
        nw.insert(value)
      }
    } else {
      if (value.y < y) {
        se.insert(value)
      } else {
        ne.insert(value)
      }
    }
  }
}

case class QtValue[T](x: Double, y: Double, value: T)

object PointQuadtree {
  private[index] val MAX_VALUES_PER_NODE = 10
}