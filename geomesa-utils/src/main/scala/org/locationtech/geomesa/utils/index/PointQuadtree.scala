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

  private val root = QtNode[T](180, 90, 360, 180) // x is lon, y is lat - offset to positive values

  override def insert(envelope: Envelope, item: T): Unit = {
    val (x, y) = getPoint(envelope)
    root.insert(QtValue(x, y, item))
  }

  override def remove(envelope: Envelope, item: T): Boolean = {
    val (x, y) = getPoint(envelope)
    root.remove(QtValue(x, y, item))
  }

  override def query(envelope: Envelope): Iterator[T] = {
    val query = new Envelope(envelope)
    query.translate(180, 90)
    root.query(query)
  }

  def getPoint(envelope: Envelope): (Double, Double) = {
    val x = ((envelope.getMinX + envelope.getMaxX) / 2) + 180
    val y = ((envelope.getMinY + envelope.getMaxY) / 2) + 90
    (x, y)
  }

  def print(): String = root.print("")
}

case class QtNode[T](x: Double, y: Double, width: Double, height: Double) {
  val extents = new Envelope(x, x, y, y)
  val values  = mutable.HashSet.empty[QtValue[T]]

  var nw: QtNode[T] = null
  var ne: QtNode[T] = null
  var sw: QtNode[T] = null
  var se: QtNode[T] = null

  def insert(item: QtValue[T]): Unit = {
    ensureExtents(x, y)
    val done = values.synchronized { // TODO sync
      if (nw == null) {
        if (values.size < PointQuadtree.MAX_VALUES_PER_NODE) {
          values.add(item)
          true
        } else {
          subdivide()
          false
        }
      } else {
        false
      }
    }
    if (!done) {
      insertIntoQuadrant(item)
    }
  }

  // should only be accessed inside value sync block
  private def subdivide(): Unit = {
    // TODO this will infinitely recurse if we have too many of the same point...
    val newWidth = width / 2
    val newHeight = height / 2
    nw = QtNode[T](x - newWidth, y + newHeight, newWidth, newHeight)
    ne = QtNode[T](x + newWidth, y + newHeight, newWidth, newHeight)
    sw = QtNode[T](x - newWidth, y - newHeight, newWidth, newHeight)
    se = QtNode[T](x + newWidth, y - newHeight, newWidth, newHeight)
    values.foreach(insertIntoQuadrant)
    values.clear()
  }

  private def insertIntoQuadrant(item: QtValue[T]): Unit = {
    if (item.x < x) {
      if (item.y < y) {
        sw.insert(item)
      } else {
        nw.insert(item)
      }
    } else {
      if (item.y < y) {
        se.insert(item)
      } else {
        ne.insert(item)
      }
    }
  }

  def query(envelope: Envelope): Iterator[T] = {
    if (!extents.intersects(envelope)) {
      return Iterator.empty
    }
    val result = values.synchronized {
      if (nw == null) {
        values.filter { value =>
          value.x >= envelope.getMinX &&
            value.x <= envelope.getMaxX &&
            value.y >= envelope.getMinY &&
            value.y <= envelope.getMaxY
        }
      } else {
        null
      }
    }
    if (result == null) {
      new Iterator[T]() {
        var iter = Iterator.empty.asInstanceOf[Iterator[T]]
        var count = 0
        override def hasNext = {
          while (!iter.hasNext && count < 4) {
            if (count == 0) {
              iter = nw.query(envelope)
            } else if (count == 1) {
              iter = QtNode.this.ne.query(envelope)
            } else if (count == 2) {
              iter = sw.query(envelope)
            } else {
              iter = se.query(envelope)
            }
            count += 1
          }
          iter.hasNext
        }

        override def next() = iter.next()
      }
    } else {
      result.iterator.map(_.value)
    }
  }

  def remove(value: QtValue[T]): Boolean = {
    if (contained(value.x, value.y)) {
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

  private def contained(x: Double, y: Double): Boolean =
    extents.synchronized(extents.covers(x, y))

  private def ensureExtents(x: Double, y: Double): Unit =
    extents.synchronized(extents.expandToInclude(x, y))

  def print(indent: String): String = {
    if (nw == null) {
      s"$indent($x $y)[ LEAF[${values.mkString(",")}] ]"
    } else {
      val tab = indent + "  "
      s"$indent($x $y)[\n${tab}NW[${nw.print(tab)}]\n${indent}NE[${ne.print(tab)}]\n${indent}SW[${sw.print(tab)}]\n${indent}SE[${se.print(tab)}]\n$indent]"
    }
  }
}

case class QtValue[T](x: Double, y: Double, value: T)

object PointQuadtree {
  private[index] val MAX_VALUES_PER_NODE = 10
}