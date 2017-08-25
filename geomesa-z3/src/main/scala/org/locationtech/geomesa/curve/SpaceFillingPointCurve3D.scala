/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon, NormalizedTime}
import org.locationtech.geomesa.curve.time.BinnedTime
import org.locationtech.geomesa.curve.time.TimePeriod.TimePeriod
import org.locationtech.sfcurve.IndexRange

/**
  * Space-time filling curve
  */
abstract class SpaceFillingPointCurve3D {

  import SpaceFillingPointCurve3D.FullPrecision

  def dx: NormalizedDimension
  def dy: NormalizedDimension
  def dz: NormalizedDimension

  def index(x: Double, y: Double, z: Double): Long
  def invert(i: Long): (Double, Double, Double)

  def ranges(x: (Double, Double), y: (Double, Double), z: (Double, Double)): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, z._1, x._2, y._2, z._2)), FullPrecision, None)

  def ranges(x: (Double, Double), y: (Double, Double), z: (Double, Double), precision: Int): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, z._1, x._2, y._2, z._2)), precision, None)

  def ranges(x: (Double, Double),
             y: (Double, Double),
             z: (Double, Double),
             precision: Int,
             maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, z._1, x._2, y._2, z._2)), precision, maxRanges)

  /**
    * Gets ranges
    *
    * @param xyz sequence of bounding boxes, in the form of (xmin, ymin, zmin, xmax, ymax, zmax)
    * @param precision precision of the zvalues to consider, up to 64 bits
    * @param maxRanges rough upper bound on the number of ranges to return
    * @return
    */
  def ranges(xyz: Seq[(Double, Double, Double, Double, Double, Double)],
             precision: Int = FullPrecision,
             maxRanges: Option[Int] = None): Seq[IndexRange]
}

object SpaceFillingPointCurve3D {
  val FullPrecision: Int = 64
}
