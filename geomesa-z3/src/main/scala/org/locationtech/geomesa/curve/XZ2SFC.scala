/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.XZ2SFC.Bounds
import org.locationtech.sfcurve.IndexRange

import scala.collection.mutable.ArrayBuffer

class XZ2SFC(g: Short) {

  // TODO see if we can use Ints
  // require(g < 13, "precision > 12 is not supported by Ints")

  def index(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Long = {
    val xlo = normalizeLon(xmin)
    val xhi = normalizeLon(xmax)
    val ylo = normalizeLat(ymin)
    val yhi = normalizeLat(ymax)

    val w = xhi - xlo
    val h = yhi - ylo

    val l1 = math.floor(math.log(math.max(w, h)) / math.log(0.5)).toInt

    val l = if (math.floor((xlo / l1) + 2) * l1 <= xlo + w || math.floor((ylo / l1) + 2) * l1 <= ylo + h) l1 else l1 + 1

    sequenceCode(xlo, ylo, l)
  }

  // normalize to [0, 1]
  private def normalizeLon(x: Double): Double = (x + 180.0) / 360.0
  private def normalizeLat(y: Double): Double = (y + 90.0) / 180.0

  private def validateBounds(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Unit = {
    require(xmin <= 180.0 && xmin >= -180.00 && xmax <= 180.0 && xmax >= -180.00 &&
        ymin <= 90.0 && ymin >= -90.00 && ymax <= 90.0 && ymax >= -90.00,
      s"Bounds must be within [-180 180] [-90 90]: [$xmin $xmax] [$ymin $ymax]"
    )
  }

  private def sequenceCode(x: Double, y: Double, length: Int): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 0L

    var i = 0
    while (i < length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true,  true)  => cs +=                                              1L; xmax = xCenter; ymax = yCenter
        case (false, true)  => cs +=      (math.pow(4, g - i).toLong - 1L) / 3L + 1L; xmin = xCenter; ymax = yCenter
        case (true,  false) => cs += 2L * (math.pow(4, g - i).toLong - 1L) / 3L + 1L; xmax = xCenter; ymin = yCenter
        case (false, false) => cs += 3L * (math.pow(4, g - i).toLong - 1L) / 3L + 1L; xmin = xCenter; ymin = yCenter
      }
      i += 1
    }

    cs
  }

  def ranges(bounds: Seq[(Double, Double, Double, Double)],
             maxRanges: Option[Int] = None,
             maxRecurse: Option[Int] = Some(XZ2SFC.DefaultRecurse)): Seq[IndexRange] = {
    val normalizedBounds = bounds.map { case (xmin, ymin, xmax, ymax) =>
      validateBounds(xmin, ymin, xmax, ymax)
      Bounds(normalizeLon(xmin),  normalizeLat(ymin), normalizeLon(xmax),normalizeLat(ymax))
    }
    val rangeStop = maxRanges.getOrElse(Int.MaxValue)
    val recurseStop = math.min(maxRecurse.getOrElse(XZ2SFC.DefaultRecurse), g)
    ranges(normalizedBounds, rangeStop, recurseStop)
  }

  private def ranges(bounds: Seq[Bounds], rangeStop: Int, recurseStop: Int): Seq[IndexRange] = {

    import XZ2SFC.LevelTerminator

    // stores our results - initial size of 100 in general saves us some re-allocation
    val ranges = new java.util.ArrayList[IndexRange](100)

    // values remaining to process - initial size of 100 in general saves us some re-allocation
    val remaining = new java.util.ArrayDeque[Bounds](100)

    // checks if a range is contained in the search space
    def isContained(quad: Bounds): Boolean = {
      var i = 0
      while (i < bounds.length) {
        if (bounds(i).contains(quad)) {
          return true
        }
        i += 1
      }
      false
    }

    // checks if a range overlaps the search space
    def isOverlapped(quad: Bounds): Boolean = {
      var i = 0
      while (i < bounds.length) {
        if (bounds(i).overlaps(quad)) {
          return true
        }
        i += 1
      }
      false
    }

    // checks a single value and either:
    //   eliminates it as out of bounds
    //   adds it to our results as fully matching, or
    //   queues up it's children for further processing
    def checkValue(quad: Bounds, level: Short): Unit = {
      if (isContained(quad)) {
        // whole range matches, happy day
        val min = sequenceCode(quad.xmin, quad.ymin, level)
        val max = sequenceCode(quad.xmax, quad.ymax, level)
        ranges.add(IndexRange(min, max, contained = true))
      } else if (isOverlapped(quad)) {
        // some portion of this range is excluded
        // queue up each sub-range for processing
        quad.children.foreach(remaining.add)
      }
    }

    // initial level
    Bounds(0.0, 0.0, 1.0, 1.0).children.foreach(remaining.add)
    remaining.add(LevelTerminator)

    // level of recursion
    var level: Short = 1

    while (level <= recurseStop && !remaining.isEmpty && ranges.size < rangeStop) {
      val next = remaining.poll
      if (next.eq(LevelTerminator)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty) {
          level = (level + 1).toShort
          remaining.add(LevelTerminator)
        }
      } else {
        checkValue(next, level)
      }
    }

    // bottom out and get all the ranges that partially overlapped but we didn't fully process
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(LevelTerminator)) {
        level = (level + 1).toShort
      } else {
        val min = sequenceCode(next.xmin, next.ymin, level)
        val max = sequenceCode(next.xmax, next.ymax, level)
        ranges.add(IndexRange(min, max, contained = false))
      }
    }

    // TODO we could use the algorithm from the XZ paper instead
    // we've got all our ranges - now reduce them down by merging overlapping values
    ranges.sort(IndexRange.IndexRangeIsOrdered)
println(ranges)
    var current = ranges.get(0) // note: should always be at least one range
    val result = ArrayBuffer.empty[IndexRange]
    var i = 1
    while (i < ranges.size()) {
      val range = ranges.get(i)
      if (range.lower <= current.upper + 1) {
        // merge the two ranges
        current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
      } else {
        // append the last range and set the current range for future merging
        result.append(current)
        current = range
      }
      i += 1
    }
    // append the last range - there will always be one left that wasn't added
    result.append(current)

    result
  }

//  private def quadrantSequence(x: Double, y: Double, length: Int): Array[Short] = {
//    var xmin = 0.0
//    var xmax = 360.0
//    var ymin = 0.0
//    var ymax = 180.0
//
//    val result = Array.ofDim[Short](length)
//
//    var i = 0
//    while (i < length) {
//      val xCenter = xmax - xmin
//      val yCenter = ymax - ymin
//      (x < xCenter, y < yCenter) match {
//        case (true, true)   => result(i) = 0; xmax = xCenter; ymax = yCenter
//        case (false, true)  => result(i) = 1; xmin = xCenter; ymax = yCenter
//        case (true, false)  => result(i) = 2; xmax = xCenter; ymin = yCenter
//        case (false, false) => result(i) = 3; xmin = xCenter; ymin = yCenter
//      }
//      i += 1
//    }
//
//    result
//  }
}

object XZ2SFC {
  val DefaultRecurse = 7
  // indicator that we have searched a full level of the quad/oct tree
  private val LevelTerminator = Bounds(-1.0, -1.0, -1.0, -1.0)

  private case class Bounds(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

    def contains(value: Bounds) = xmin <= value.xmin && xmax >= value.xmax && ymin <= value.ymin && ymax >= value.ymax

    def overlaps(value: Bounds) = xmin <= value.xmax && xmax >= value.xmin && ymin <= value.ymax && ymax >= value.ymin

    def children: Seq[Bounds] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter)
      val c1 = copy(xmin = xCenter, ymax = yCenter)
      val c2 = copy(xmax = xCenter, ymin = yCenter)
      val c3 = copy(xmin = xCenter, ymin = yCenter)
println((c0, c1, c2, c3))
      Seq(c0, c1, c2, c3)
    }
  }
}