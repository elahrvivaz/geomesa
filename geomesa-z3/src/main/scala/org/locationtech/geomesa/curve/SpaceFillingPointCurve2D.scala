/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.locationtech.sfcurve.IndexRange

/**
  * Space filling curve
  */
abstract class SpaceFillingPointCurve2D {

  import SpaceFillingPointCurve2D.FullPrecision

  def dx: NormalizedDimension
  def dy: NormalizedDimension

  def index(x: Double, y: Double): Long
  def invert(i: Long): (Double, Double)

  def ranges(x: (Double, Double), y: (Double, Double)): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), FullPrecision, None)

  def ranges(x: (Double, Double), y: (Double, Double), precision: Int): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), precision, None)

  def ranges(x: (Double, Double), y: (Double, Double), precision: Int, maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), precision, maxRanges)

  /**
    * Gets ranges
    *
    * @param xy sequence of bounding boxes, in the form of (xmin, ymin, xmax, ymax)
    * @param precision precision of the zvalues to consider, up to 64 bits
    * @param maxRanges rough upper bound on the number of ranges to return
    * @return
    */
  def ranges(xy: Seq[(Double, Double, Double, Double)],
             precision: Int = FullPrecision,
             maxRanges: Option[Int] = None): Seq[IndexRange]
}

object SpaceFillingPointCurve2D {
  val FullPrecision: Int = 64
}
