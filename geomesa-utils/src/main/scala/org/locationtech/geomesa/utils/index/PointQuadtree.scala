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

  throw new NotImplementedError("Not yet finished") // TODO finish implementing this

  private val root = QtNode[T](0, 0, 360, 180) // x is lon, y is lat

  override def insert(envelope: Envelope, item: T): Unit = {
    val (x, y) = SpatialIndex.getCenter(envelope)
    root.insert(x, y, item)
  }

  override def remove(envelope: Envelope, item: T): Boolean = {
    val (x, y) = SpatialIndex.getCenter(envelope)
    root.remove(x, y, item)
  }

  override def query(envelope: Envelope): Iterator[T] = root.query(envelope)

  def print(): String = root.print("")
}

case class QtNode[T](centerX: Double, centerY: Double, width: Double, height: Double) {
  val extents = new Envelope(centerX, centerX, centerY, centerY)
  val values  = mutable.Map.empty[(Double, Double), mutable.HashSet[T]]

  var nw: QtNode[T] = null
  var ne: QtNode[T] = null
  var sw: QtNode[T] = null
  var se: QtNode[T] = null

  def insert(x: Double, y: Double, item: T): Unit = {
    ensureExtents(x, y)
    val done = values.synchronized { // TODO sync
      if (nw == null) {
        if (values.size < PointQuadtree.MAX_VALUES_PER_NODE) {
          values.getOrElse((x, y), mutable.HashSet.empty[T]).add(item)
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
      insertIntoQuadrant(x, y, item)
    }
  }

  // should only be accessed inside value sync block
  private def subdivide(): Unit = {
    val newWidth = width / 2
    val newHeight = height / 2
    nw = QtNode[T](centerX - newWidth, centerY + newHeight, newWidth, newHeight)
    ne = QtNode[T](centerX + newWidth, centerY + newHeight, newWidth, newHeight)
    sw = QtNode[T](centerX - newWidth, centerY - newHeight, newWidth, newHeight)
    se = QtNode[T](centerX + newWidth, centerY - newHeight, newWidth, newHeight)
    values.foreach { case (pt, items) => items.foreach(insertIntoQuadrant(pt._1, pt._2, _)) }
    values.clear()
  }

  private def insertIntoQuadrant(x: Double, y: Double, item: T): Unit = {
    if (x < centerX) {
      if (y < centerY) {
        sw.insert(x, y, item)
      } else {
        nw.insert(x, y, item)
      }
    } else {
      if (y < centerY) {
        se.insert(x, y, item)
      } else {
        ne.insert(x, y, item)
      }
    }
  }

  def query(envelope: Envelope): Iterator[T] = {
    if (!extents.intersects(envelope)) {
      return Iterator.empty
    }
    val result = values.synchronized {
      if (nw == null) {
        values.flatMap { case (pt, items) =>
          val (x, y) = pt
          if (x >= envelope.getMinX && x <= envelope.getMaxX &&
              y >= envelope.getMinY && y <= envelope.getMaxX) {
            items.toSeq
          } else {
            Seq.empty
          }
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
      result.iterator
    }
  }

  def remove(x: Double, y: Double, item: T): Boolean = {
    if (contained(x, y)) {
      val (removed, checkQuadrants) = values.synchronized {
        if (nw == null) {
          (values.get((x, y)).exists(_.remove(item)), false)
        } else {
          (false, true)
        }
      }
      if (checkQuadrants) {
        nw.remove(x, y, item) || ne.remove(x, y, item) || sw.remove(x, y, item) || se.remove(x, y, item)
      } else {
        removed
      }
    } else {
      false
    }
  }

  private def contained(x: Double, y: Double): Boolean = extents.synchronized(extents.covers(x, y))

  private def ensureExtents(x: Double, y: Double): Unit = extents.synchronized(extents.expandToInclude(x, y))

  def print(indent: String): String = {
    if (nw == null) {
      s"$indent($centerX $centerX)[ LEAF[${values.mkString(",")}] ]"
    } else {
      val tab = indent + "  "
      s"$indent($centerX $centerX)[\n${tab}NW[${nw.print(tab)}]\n${indent}NE[${ne.print(tab)}]\n${indent}SW[${sw.print(tab)}]\n${indent}SE[${se.print(tab)}]\n$indent]"
    }
  }
}

case class QtValue[T](x: Double, y: Double, value: T)

object PointQuadtree {
  private[index] val MAX_VALUES_PER_NODE = 10
}