/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.{Longs, Shorts}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range => AccRange, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.curve.Z3

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator.{pointsKey, zKey}

  var source: SortedKeyValueIterator[Key, Value] = null
  var zNums: Array[Int] = null

  var xmin: Int = -1
  var xmax: Int = -1
  var ymin: Int = -1
  var ymax: Int = -1
  var tmin: Int = -1
  var tmax: Int = -1
  var wmin: Short = -1
  var wmax: Short = -1

  var tLo: Int = -1
  var tHi: Int = -1

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
    x >= xmin && x <= xmax && y >= ymin && y <= ymax && {
      if (week == wmin) {
        t >= tmin && t <= tHi
      } else if (week == wmax) {
        t >= tLo && t <= tmax
      } else {
        true
      }
    }
  }

  private def inBoundsNonPoints(k: Key): Boolean = {
    // TODO
    k.getRow(row)
    val bytes = row.getBytes
    val week = Shorts.fromBytes(bytes(0), bytes(1))
    // non-points only use 3 bytes of the z curve
    val zBytes = Array.fill[Byte](8)(0)
    System.arraycopy(bytes, 2, zBytes, 0, Z3Table.GEOM_Z_NUM_BYTES)
    val keyZ = Longs.fromByteArray(zBytes)
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

    zNums = options.get(zKey).split(":").map(_.toInt)
    xmin = zNums(0)
    xmax = zNums(1)
    ymin = zNums(2)
    ymax = zNums(3)
    tmin = zNums(4)
    tmax = zNums(5)
    wmin = zNums(6).toShort
    wmax = zNums(7).toShort
    tLo = if (wmin == wmax) tmin else zNums(8)
    tHi = if (wmin == wmax) tmax else zNums(9)

    // TODO
    val zs = if (isPoints) {
      stringToMap(zMap).toList.sortBy(_._1)
    } else {
      stringToMap(zMap).toList.sortBy(_._1).map { case(w, (ll, ur)) => (w, (ll & Z3Table.GEOM_Z_MASK, ur & Z3Table.GEOM_Z_MASK)) }
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
    iter.init(source, Map(pointsKey -> isPoints.toString, zKey -> zNums.mkString(":")), env)
    iter
  }
}

object Z3Iterator {

  val zKey = "z"
  val pointsKey = "g"

  def configure(isPoints: Boolean,
                xmin: Int,
                xmax: Int,
                ymin: Int,
                ymax: Int,
                tmin: Int,
                tmax: Int,
                wmin: Short,
                wmax: Short,
                tLo: Int,
                tHi: Int,
                priority: Int) = {
    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])
    is.addOption(pointsKey, isPoints.toString)
    is.addOption(zKey, s"$xmin:$xmax:$ymin:$ymax:$tmin:$tmax:$wmin:$wmax:$tLo:$tHi")
    is
  }
}