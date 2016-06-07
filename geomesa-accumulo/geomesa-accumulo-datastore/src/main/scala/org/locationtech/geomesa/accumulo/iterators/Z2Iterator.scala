/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.Longs
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.tables.Z2Table
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.sfcurve.zorder.Z2

class Z2Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z2Iterator._

  var source: SortedKeyValueIterator[Key, Value] = null

  var keyX: Array[String] = null
  var keyY: Array[String] = null

  var xvals: Array[(Int, Int)] = null
  var yvals: Array[(Int, Int)] = null

  var isPoints: Boolean = false
  var isTableSharing: Boolean = false
  var rowToZ: Array[Byte] => Long = null

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)

    isPoints = options.get(PointsKey).toBoolean
    isTableSharing = options.get(TableSharingKey).toBoolean

    keyX = options.get(ZKeyX).split(RangeSeparator)
    keyY = options.get(ZKeyY).split(RangeSeparator)

    xvals = keyX.map(_.toInt).grouped(2).map { case Array(x1, x2) => (x1, x2) }.toArray
    yvals = keyY.map(_.toInt).grouped(2).map { case Array(y1, y2) => (y1, y2) }.toArray

    // account for shard and table sharing bytes
    val numBytes = if (isPoints) 8 else Z2Table.GEOM_Z_NUM_BYTES
    rowToZ = rowToZ(numBytes, isTableSharing)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop) {
      if (inBounds(source.getTopKey)) {
        topKey = source.getTopKey
        topValue = source.getTopValue
        return
      } else {
        source.next()
      }
    }
  }

  private def inBounds(k: Key): Boolean = {
    k.getRow(row)
    val keyZ = rowToZ(row.getBytes)
    Z2Iterator.inBounds(Z2(keyZ).d0, xvals) && Z2Iterator.inBounds(Z2(keyZ).d1, yvals)
  }

  private def rowToZ(count: Int, tableSharing: Boolean): (Array[Byte]) => Long = (count, tableSharing) match {
    case (3, true)  => (bb) => Longs.fromBytes(bb(2), bb(3), bb(4), 0, 0, 0, 0, 0)
    case (3, false) => (bb) => Longs.fromBytes(bb(1), bb(2), bb(3), 0, 0, 0, 0, 0)
    case (4, true)  => (bb) => Longs.fromBytes(bb(2), bb(3), bb(4), bb(5), 0, 0, 0, 0)
    case (4, false) => (bb) => Longs.fromBytes(bb(1), bb(2), bb(3), bb(4), 0, 0, 0, 0)
    case (8, true)  => (bb) => Longs.fromBytes(bb(2), bb(3), bb(4), bb(5), bb(6), bb(7), bb(8), bb(9))
    case (8, false) => (bb) => Longs.fromBytes(bb(1), bb(2), bb(3), bb(4), bb(5), bb(6), bb(7), bb(8))
    case _ => throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $count")
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val iter = new Z2Iterator
    val opts = Map(ZKeyX -> keyX.mkString(RangeSeparator), ZKeyY -> keyY.mkString(RangeSeparator),
      PointsKey -> isPoints.toString, TableSharingKey -> isTableSharing.toString)
    iter.init(source, opts, env)
    iter
  }
}

object Z2Iterator {

  val ZKeyX = "zx"
  val ZKeyY = "zy"
  val PointsKey = "points"
  val TableSharingKey = "table-sharing"

  val RangeSeparator = ":"

  def configure(bounds: Seq[(Double, Double, Double, Double)],
                isPoints: Boolean,
                tableSharing: Boolean,
                priority: Int) = {

    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])

    // index space values for comparing in the iterator
    val (xOpts, yOpts) = if (isPoints) {
      val xOpts = bounds.map { case (xmin, _, xmax, _) =>
        s"${Z2SFC.lon.normalize(xmin)}$RangeSeparator${Z2SFC.lon.normalize(xmax)}"
      }
      val yOpts = bounds.map { case (_, ymin, _, ymax) =>
        s"${Z2SFC.lat.normalize(ymin)}$RangeSeparator${Z2SFC.lat.normalize(ymax)}"
      }
      (xOpts, yOpts)
    } else {
      val normalized = bounds.map { case (xmin, ymin, xmax, ymax) =>
        val (lx, ly) = decodeNonPoints(xmin, ymin)
        val (ux, uy) = decodeNonPoints(xmax, ymax)
        (s"$lx$RangeSeparator$ux", s"$ly$RangeSeparator$uy")
      }
      (normalized.map(_._1), normalized.map(_._2))
    }

    is.addOption(ZKeyX, xOpts.mkString(RangeSeparator))
    is.addOption(ZKeyY, yOpts.mkString(RangeSeparator))
    is.addOption(PointsKey, isPoints.toString)
    is.addOption(TableSharingKey, tableSharing.toString)
    is
  }

  private def decodeNonPoints(x: Double, y: Double): (Int, Int) =
    Z2(Z2SFC.index(x, y).z & Z2Table.GEOM_Z_MASK).decode

  private [iterators] def inBounds(value: Int, bounds: Array[(Int, Int)]): Boolean = {
    var i = 0
    while (i < bounds.length) {
      val (min, max) = bounds(i)
      if (value >= min && value <= max) {
        return true
      }
      i += 1
    }
    false
  }
}
