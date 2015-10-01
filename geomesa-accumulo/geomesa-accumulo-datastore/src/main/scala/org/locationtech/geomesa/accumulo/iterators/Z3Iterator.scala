/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.{Shorts, Longs}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range => AccRange, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.curve.Z3

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator.{pointsKey, zKey, stringToMap}

  var source: SortedKeyValueIterator[Key, Value] = null

  var zMap: String = null
  var zsByWeek: Array[(Int, Int, Int, Int, Int, Int)] = null
  var weekOffset: Short = -1
  var isPoints: Boolean = false
  var inBounds: (Key) => Boolean = null

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop && !inBounds(source.getTopKey)) { source.next() }
    if (source.hasTop) {
      topKey = source.getTopKey
      topValue = source.getTopValue
    }
  }

  private def inBoundsPoints(k: Key): Boolean = {
    k.getRow(row)
    val bytes = row.getBytes
    val week = Shorts.fromBytes(bytes(0), bytes(1))
    val keyZ = Longs.fromBytes(bytes(2), bytes(3), bytes(4), bytes(5), bytes(6), bytes(7), bytes(8), bytes(9))
    val (x, y, t) = Z3(keyZ).decode
    val (xmin, ymin, tmin, xmax, ymax, tmax) = zsByWeek(week - weekOffset)
    x >= xmin && x <= xmax && y >= ymin && y <= ymax && t >= tmin && t <= tmax
  }

  private def inBoundsNonPoints(k: Key): Boolean = {
    k.getRow(row)
    val bytes = row.getBytes
    val week = Shorts.fromBytes(bytes(0), bytes(1))
    // non-points only use 3 bytes of the z curve
    val keyZ = Longs.fromBytes(bytes(2), bytes(3), bytes(4), 0, 0, 0, 0, 0)
    val (x, y, t) = Z3(keyZ).decode
    val (xmin, ymin, tmin, xmax, ymax, tmax) = zsByWeek(week - weekOffset)
    x >= xmin && x <= xmax && y >= ymin && y <= ymax && t >= tmin && t <= tmax
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)
    isPoints = options.get(pointsKey).toBoolean
    zMap = options.get(zKey)
    val zs = if (isPoints) {
      stringToMap(zMap).toList.sortBy(_._1)
    } else {
      stringToMap(zMap).toList.sortBy(_._1).map { case(w, (ll, ur)) => (w, (ll & 0xffffff0000000000L, ur & 0xffffff0000000000L)) }
    }
    weekOffset = zs.head._1
    // NB: we assume weeks are continuous
    zsByWeek = zs.map(_._2).toArray.map { case (ll, ur) =>
      val (x0, y0, t0) = Z3(ll).decode
      val (x1, y1, t1) = Z3(ur).decode
      (x0, y0, t0, x1, y1, t1)
    }
    inBounds = if (isPoints) inBoundsPoints else inBoundsNonPoints
  }

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val iter = new Z3Iterator
    iter.init(source, Map(zKey -> zMap, pointsKey -> isPoints.toString), env)
    iter
  }
}

object Z3Iterator {

  val zKey = "z"
  val pointsKey = "g"

  def configure(zRangesByWeek: Map[Short, (Long, Long)], isPoints: Boolean, priority: Int) = {
    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])
    is.addOption(zKey, mapToString(zRangesByWeek))
    is.addOption(pointsKey, isPoints.toString)
    is
  }

  protected[iterators] def mapToString(ranges: Map[Short, (Long, Long)]): String =
    ranges.map { case (w, (ll, ur)) => s"$w-$ll-$ur" }.mkString(",")

  protected[iterators] def stringToMap(ranges: String): Map[Short, (Long, Long)] =
    ranges.split(",").map(_.split("-")).map(r => r.head.toShort -> (r(1).toLong, r(2).toLong)).toMap
}