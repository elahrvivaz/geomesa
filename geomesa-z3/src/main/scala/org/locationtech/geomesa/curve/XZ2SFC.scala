/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.XZ2SFC.{QueryWindow, XElement}
import org.locationtech.sfcurve.IndexRange

import scala.collection.mutable.ArrayBuffer

/**
  * Extended Z-order curve implementation used for efficiently storing polygons.
  *
  * Based on 'XZ-Ordering: A Space-Filling Curve for Objects with Spatial Extension'
  * by Christian Böhm, Gerald Klump  and Hans-Peter Kriegel
  *
  * @param g resolution level of the curve - i.e. how many times the space will be recursively quartered
  */
class XZ2SFC(g: Short) {

  // TODO see what the max value of g can be where we can use Ints instead of Longs and possibly refactor to use Ints

  /**
    * Index a polygon by it's bounding box
    *
    * @param bounds (xmin, ymin, xmax, ymax)
    * @return z value for the bounding box
    */
  def index(bounds: (Double, Double, Double, Double)): Long = index(bounds._1, bounds._2, bounds._3, bounds._4)

  /**
    * Index a polygon by it's bounding box
    *
    * @param xmin min x value in [-180,180]
    * @param ymin min y value in [-90,90]
    * @param xmax max x value in [-180,180], must be >= xmin
    * @param ymax max y value in [-90,90], must be >= ymin
    * @return z value for the bounding box
    */
  def index(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Long = {
    // normalize inputs to [0,1]
    val (nxmin, nymin, nxmax, nymax) = XZ2SFC.normalize(xmin, ymin, xmax, ymax)

    // calculate the length of the sequence code (section 4.1 of XZ-Ordering paper)

    val w = nxmax - nxmin
    val h = nymax - nymin

    val (wh, lower, upper) = if (w > h) (w, nxmin, nxmax) else (h, nymin, nymax)

    // l1 (el-one) is a bit confusing to read, but corresponds with the paper's definitions
    val l1 = math.min(g, math.floor(math.log(wh) / XZ2SFC.LogPointFive).toInt)

    // predicate for checking how many axis the polygon intersects
    val length = if (math.floor((lower / l1) + 2) * l1 <= upper) l1 else l1 + 1

    sequenceCode(nxmin, nymin, length)
  }

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query a windows to cover in the form (xmin, ymin, xmax, ymax) where:
    *                xmin, xmax are both in [-180,180]
    *                ymin, ymax are both in [-90,90]
    *                xmax >= xmin
    *                ymax >= ymin
    * @return
    */
  def ranges(query: (Double, Double, Double, Double)): Seq[IndexRange] = ranges(Seq(query))

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query a windows to cover in the form (xmin, ymin, xmax, ymax) where:
    *                xmin, xmax are both in [-180,180]
    *                ymin, ymax are both in [-90,90]
    *                xmax >= xmin
    *                ymax >= ymin
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(query: (Double, Double, Double, Double), maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq(query), maxRanges)

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param xmin min x value in [-180,180]
    * @param ymin min y value in [-90,90]
    * @param xmax max x value in [-180,180], >= xmin
    * @param ymax max y value in [-90,90], >= ymin
    * @return
    */
  def ranges(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, xmax, ymax)))

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param xmin min x value in [-180,180]
    * @param ymin min y value in [-90,90]
    * @param xmax max x value in [-180,180], >= xmin
    * @param ymax max y value in [-90,90], >= ymin
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(xmin: Double, ymin: Double, xmax: Double, ymax: Double, maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, xmax, ymax)), maxRanges)

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param queries a sequence of OR'd windows to cover. Each window is in the form (xmin, ymin, xmax, ymax) where:
    *                  xmin, xmax are both in [-180,180]
    *                  ymin, ymax are both in [-90,90]
    *                  xmax >= xmin
    *                  ymax >= ymin
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(queries: Seq[(Double, Double, Double, Double)], maxRanges: Option[Int] = None): Seq[IndexRange] = {
    // normalize inputs to [0,1]
    val windows = queries.map { case (xmin, ymin, xmax, ymax) =>
      val (nxmin, nymin, nxmax, nymax) = XZ2SFC.normalize(xmin, ymin, xmax, ymax)
      QueryWindow(nxmin, nymin, nxmax, nymax)
    }
    ranges(windows.toArray, maxRanges.getOrElse(Int.MaxValue))
  }

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query a sequence of OR'd windows to cover, normalized to [0,1]
    * @param rangeStop a rough max value for the number of ranges to return
    * @return
    */
  private def ranges(query: Array[QueryWindow], rangeStop: Int): Seq[IndexRange] = {

    import XZ2SFC.{LevelOneElements, LevelTerminator}

    // stores our results - initial size of 100 in general saves us some re-allocation
    val ranges = new java.util.ArrayList[IndexRange](100)

    // values remaining to process - initial size of 100 in general saves us some re-allocation
    val remaining = new java.util.ArrayDeque[XElement](100)

    // checks if a quad is contained in the search space
    def isContained(quad: XElement): Boolean = {
      var i = 0
      while (i < query.length) {
        if (quad.isContained(query(i))) {
          return true
        }
        i += 1
      }
      false
    }

    // checks if a quad overlaps the search space
    def isOverlapped(quad: XElement): Boolean = {
      var i = 0
      while (i < query.length) {
        if (quad.overlaps(query(i))) {
          return true
        }
        i += 1
      }
      false
    }

    // checks a single value and either:
    //   eliminates it as out of bounds
    //   adds it to our results as fully matching, or
    //   adds it to our results as partial matching and queues up it's children for further processing
    def checkValue(quad: XElement, level: Short): Unit = {
      if (isContained(quad)) {
        // whole range matches, happy day
        val (min, max) = sequenceInterval(quad.xmin, quad.ymin, level, partial = false)
        ranges.add(IndexRange(min, max, contained = true))
      } else if (isOverlapped(quad)) {
        // some portion of this range is excluded
        // add the partial match and queue up each sub-range for processing
        val (min, max) = sequenceInterval(quad.xmin, quad.ymin, level, partial = true)
        ranges.add(IndexRange(min, max, contained = false))
        quad.children.foreach(remaining.add)
      }
    }

    // initial level
    LevelOneElements.foreach(remaining.add)
    remaining.add(LevelTerminator)

    // level of recursion
    var level: Short = 1

    while (level < g && !remaining.isEmpty && ranges.size < rangeStop) {
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
      val quad = remaining.poll
      if (quad.eq(LevelTerminator)) {
        level = (level + 1).toShort
      } else {
        val (min, max) = sequenceInterval(quad.xmin, quad.ymin, level, partial = false)
        ranges.add(IndexRange(min, max, contained = false))
      }
    }

    // we've got all our ranges - now reduce them down by merging overlapping values
    // note: we don't bother reducing the ranges as in the XZ paper, as accumulo handles lots of ranges fairly well
    ranges.sort(IndexRange.IndexRangeIsOrdered)

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

  /**
    * Computes the sequence code for a given point - for polygons this is the lower-left corner.
    *
    * Based on Definition 2 from the XZ-Ordering paper
    *
    * @param x normalized x value [0,1]
    * @param y normalized y value [0,1]
    * @param length length of the sequence code that will be generated
    * @return
    */
  private def sequenceCode(x: Double, y: Double, length: Int): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 1L

    var i = 0
    while (i < length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true,  true)  =>                                                   xmax = xCenter; ymax = yCenter
        case (false, true)  => cs += 1L * (math.pow(4, g - i).toLong - 1L) / 3L; xmin = xCenter; ymax = yCenter
        case (true,  false) => cs += 2L * (math.pow(4, g - i).toLong - 1L) / 3L; xmax = xCenter; ymin = yCenter
        case (false, false) => cs += 3L * (math.pow(4, g - i).toLong - 1L) / 3L; xmin = xCenter; ymin = yCenter
      }
      i += 1
    }

    cs
  }

  /**
    * Computes an interval of sequence codes for a given point - for polygons this is the lower-left corner.
    *
    * @param x normalized x value [0,1]
    * @param y normalized y value [0,1]
    * @param length length of the sequence code that will used as the basis for this interval
    * @param partial true if the element partially intersects the query window, false if it is fully contained
    * @return
    */
  private def sequenceInterval(x: Double, y: Double, length: Short, partial: Boolean): (Long, Long) = {
    val min = sequenceCode(x, y, length)
    // if a partial match, we just use the single sequence code as an interval
    // if a full match, we have to match all sequence codes starting with the single sequence code
    val max = if (partial) { min } else {
      // from lemma 3 in the XZ-Ordering paper
      min + (math.pow(4, g - length + 1).toLong - 1L) / 3L
    }
    (min, max)
  }
}

object XZ2SFC {

  val QueryMinX = -180.0
  val QueryMinY = -90.0
  val QueryMaxX = 180.0
  val QueryMaxY = 90.0

  val LogPointFive = math.log(0.5)

  private val QueryRangeX = QueryMaxX - QueryMinX
  private val QueryRangeY = QueryMaxY - QueryMinY

  // the initial level of quads
  private val LevelOneElements = XElement(0.0, 0.0, 1.0, 1.0).children

  // indicator that we have searched a full level of the quad/oct tree
  private val LevelTerminator = XElement(-1.0, -1.0, -1.0, -1.0)

  /**
    * Normalize lat/lon to [0,1]
    *
    * @param xmin min x value in [-180,180]
    * @param ymin min y value in [-90,90]
    * @param xmax max x value in [-180,180], must be >= xmin
    * @param ymax max y value in [-90,90], must be >= ymin
    * @return
    */
  def normalize(xmin: Double, ymin: Double, xmax: Double, ymax: Double): (Double, Double, Double, Double) = {
    require(xmin <= QueryMaxX && xmin >= QueryMinX && xmax <= QueryMaxX && xmax >= QueryMinX &&
        ymin <= QueryMaxY && ymin >= QueryMinY && ymax <= QueryMaxY && ymax >= QueryMinY,
      s"Bounds must be within [$QueryMinX $QueryMaxX] [$QueryMinY $QueryMaxY]: [$xmin $xmax] [$ymin $ymax]"
    )
    require(xmin <= xmax && ymin <= ymax, s"Bounds must be ordered: [$xmin $xmax] [$ymin $ymax]")
    val nxmin = (xmin + QueryMaxX) / QueryRangeX
    val nymin = (ymin + QueryMaxY) / QueryRangeY
    val nxmax = (xmax + QueryMaxX) / QueryRangeX
    val nymax = (ymax + QueryMaxY) / QueryRangeY
    (nxmin, nymin, nxmax, nymax)
  }

  /**
    * Region being queried. Bounds are normalized to [0-1].
    *
    * @param xmin x lower bound in [0-1]
    * @param ymin y lower bound in [0-1]
    * @param xmax x upper bound in [0-1], must be >= xmin
    * @param ymax y upper bound in [0-1], must be >= ymin
    */
  private case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double)

  /**
    * An extended Z curve element. Bounds refer to the non-extended z element for simplicity of calculation.
    *
    * An extended Z element refers to a normal Z curve element that has it's upper bounds expanded by double it's
    * width/height. By convention, an element is always square.
    *
    * @param xmin x lower bound in [0-1]
    * @param ymin y lower bound in [0-1]
    * @param xmax x upper bound in [0-1], must be >= xmin
    * @param ymax y upper bound in [0-1], must be >= ymin
    */
  private case class XElement(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

    // length of the non-extended side (note: by convention width should be equal to height)
    lazy private val length = xmax - xmin

    // extended x and y bounds
    lazy val xext = xmax + length
    lazy val yext = ymax + length

    def isContained(window: QueryWindow): Boolean =
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xext && window.ymax >= yext

    def overlaps(window: QueryWindow): Boolean =
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xext && window.ymin <= yext

    def children: Seq[XElement] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter)
      val c1 = copy(xmin = xCenter, ymax = yCenter)
      val c2 = copy(xmax = xCenter, ymin = yCenter)
      val c3 = copy(xmin = xCenter, ymin = yCenter)
      Seq(c0, c1, c2, c3)
    }
  }
}
