/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.{Longs, Shorts}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.index.Z3IdxStrategy
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.sfcurve.zorder.Z3

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator._

  var source: SortedKeyValueIterator[Key, Value] = null

  var keyX: Array[String] = null
  var keyY: Array[String] = null
  var keyT: Array[String] = null

  var xvals: Array[(Int, Int)] = null
  var yvals: Array[(Int, Int)] = null
  var tvals: Array[Array[(Int, Int)]] = null

  var minWeek: Short = Short.MinValue
  var maxWeek: Short = Short.MinValue

  var isPoints: Boolean = false
  var hasSplits: Boolean = false
  var rowToWeekZ: Array[Byte] => (Short, Long) = null

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)

    isPoints = options.get(PointsKey).toBoolean

    keyX = options.get(ZKeyX).split(RangeSeparator)
    keyY = options.get(ZKeyY).split(RangeSeparator)
    keyT = options.get(ZKeyT).split(WeekSeparator).filterNot(_.isEmpty)

    xvals = keyX.map(_.toInt).grouped(2).map { case Array(x1, x2) => (x1, x2) }.toArray
    yvals = keyY.map(_.toInt).grouped(2).map { case Array(y1, y2) => (y1, y2) }.toArray

    minWeek = Short.MinValue
    maxWeek = Short.MinValue

    val weeksAndTimes = keyT.map { times =>
      val parts = times.split(RangeSeparator)
      val week = parts(0).toShort
      // set min/max weeks - note: side effect in map
      if (minWeek == Short.MinValue) {
        minWeek = week
        maxWeek = week
      } else if (week < minWeek) {
        minWeek = week
      } else if (week > maxWeek) {
        maxWeek = week
      }
      (week, parts.drop(1).grouped(2).map { case Array(t1, t2) => (t1.toInt, t2.toInt) }.toArray)
    }

    tvals = if (minWeek == Short.MinValue) Array.empty else Array.ofDim(maxWeek - minWeek + 1)
    weeksAndTimes.foreach { case (w, times) => tvals(w - minWeek) = times }

    hasSplits = options.get(SplitsKey).toBoolean
    val count = if (isPoints) 8 else Z3Table.GEOM_Z_NUM_BYTES
    rowToWeekZ = rowToWeekZ(count, hasSplits)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  private def findTop(): Unit = {
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
    val (week, keyZ) = rowToWeekZ(row.getBytes)
    Z2Iterator.inBounds(Z3(keyZ).d0, xvals) && Z2Iterator.inBounds(Z3(keyZ).d1, yvals) && {
      // we know we're only going to scan appropriate weeks, so leave out whole weeks
      if (week > maxWeek || week < minWeek) { true } else {
        val times = tvals(week - minWeek)
        times == null || Z2Iterator.inBounds(Z3(keyZ).d2, times)
      }
    }
  }

  private def rowToWeekZ(count: Int, splits: Boolean): (Array[Byte]) => (Short, Long) = {
    val zBytes = Longs.fromBytes _
    val wBytes = Shorts.fromBytes _
    (count, splits) match {
      case (3, true)  => (bb) => (wBytes(bb(1), bb(2)), zBytes(bb(3), bb(4), bb(5), 0, 0, 0, 0, 0))
      case (3, false) => (bb) => (wBytes(bb(0), bb(1)), zBytes(bb(2), bb(3), bb(4), 0, 0, 0, 0, 0))
      case (4, true)  => (bb) => (wBytes(bb(1), bb(2)), zBytes(bb(3), bb(4), bb(5), bb(6), 0, 0, 0, 0))
      case (4, false) => (bb) => (wBytes(bb(0), bb(1)), zBytes(bb(2), bb(3), bb(4), bb(5), 0, 0, 0, 0))
      case (8, true)  => (bb) => (wBytes(bb(1), bb(2)), zBytes(bb(3), bb(4), bb(5), bb(6), bb(7), bb(8), bb(9), bb(10)))
      case (8, false) => (bb) => (wBytes(bb(0), bb(1)), zBytes(bb(2), bb(3), bb(4), bb(5), bb(6), bb(7), bb(8), bb(9)))
      case _ => throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $count")
    }
  }

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val iter = new Z3Iterator
    val opts = Map(ZKeyX -> keyX.mkString(RangeSeparator), ZKeyY -> keyY.mkString(RangeSeparator),
      ZKeyT -> keyT.mkString(WeekSeparator), PointsKey -> isPoints.toString, SplitsKey -> hasSplits.toString)
    iter.init(source, opts, env)
    iter
  }
}

object Z3Iterator {

  val ZKeyX = "zx"
  val ZKeyY = "zy"
  val ZKeyT = "zt"

  val PointsKey = "points"
  val SplitsKey = "splits"

  val RangeSeparator = ":"
  val WeekSeparator = ";"

  def configure(bounds: Seq[(Double, Double, Double, Double)],
                timesByWeek: Map[Short, Seq[(Long, Long)]],
                isPoints: Boolean,
                hasSplits: Boolean,
                priority: Int) = {

    import Z3IdxStrategy.MinTime

    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])

    // index space values for comparing in the iterator
    val (xOpts, yOpts, tOpts) = if (isPoints) {
      val xOpts = bounds.map { case (xmin, _, xmax, _) =>
        s"${Z3SFC.lon.normalize(xmin)}$RangeSeparator${Z3SFC.lon.normalize(xmax)}"
      }
      val yOpts = bounds.map { case (_, ymin, _, ymax) =>
        s"${Z3SFC.lat.normalize(ymin)}$RangeSeparator${Z3SFC.lat.normalize(ymax)}"
      }
      // we know we're only going to scan appropriate weeks, so leave out whole weeks
      val tOpts = timesByWeek.filter(_._2 != Z3IdxStrategy.WholeWeek).toSeq.sortBy(_._1).map { case (w, times) =>
        val time = times.map { case (t1, t2) =>
          s"${Z3SFC.time.normalize(t1)}$RangeSeparator${Z3SFC.time.normalize(t2)}"
        }
        s"$w$RangeSeparator${time.mkString(RangeSeparator)}"
      }
      (xOpts, yOpts, tOpts)
    } else {
      val normalized = bounds.map { case (xmin, ymin, xmax, ymax) =>
        val (lx, ly, _) = decodeNonPoints(xmin, ymin, MinTime) // note: time is not used
        val (ux, uy, _) = decodeNonPoints(xmax, ymax, MinTime) // note: time is not used
        (s"$lx$RangeSeparator$ux", s"$ly$RangeSeparator$uy")
      }

      // we know we're only going to scan appropriate weeks, so leave out whole weeks
      val tOpts = timesByWeek.filter(_._2 == Z3IdxStrategy.WholeWeek).toSeq.sortBy(_._1).map { case (w, times) =>
        val time = times.map { case (t1, t2) =>
          s"${decodeNonPoints(0, 0, t1)._3}$RangeSeparator${decodeNonPoints(0, 0, t2)._3}"
        }
        s"$w$RangeSeparator${time.mkString(RangeSeparator)}"
      }
      (normalized.map(_._1), normalized.map(_._2), tOpts)
    }

    is.addOption(ZKeyX, xOpts.mkString(RangeSeparator))
    is.addOption(ZKeyY, yOpts.mkString(RangeSeparator))
    is.addOption(ZKeyT, tOpts.mkString(WeekSeparator))
    is.addOption(PointsKey, isPoints.toString)
    is.addOption(SplitsKey, hasSplits.toString)

    is
  }

  private def decodeNonPoints(x: Double, y: Double, t: Long): (Int, Int, Int) =
    Z3(Z3SFC.index(x, y, t).z & Z3Table.GEOM_Z_MASK).decode
}