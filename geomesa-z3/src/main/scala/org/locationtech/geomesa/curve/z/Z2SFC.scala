/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.z

import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.locationtech.geomesa.curve.{NormalizedDimension, SpaceFillingPointCurve2D}
import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.{Z2, ZRange}

/**
  * z2 space-filling curve
  *
  * @param precision number of bits used per dimension - note sum must be less than 64
  */
class Z2SFC(precision: Int) extends SpaceFillingPointCurve2D {

  require(precision > 0 && precision < 32, "Precision (bits) per dimension must be in [1,31]")

  override val dx: NormalizedDimension = NormalizedLon(precision)
  override val dy: NormalizedDimension = NormalizedLat(precision)

  override def index(x: Double, y: Double): Long = {
    require(x >= dx.min && x <= dx.max && y >= dy.min && y <= dy.max,
      s"Value(s) out of bounds ([${dx.min},${dx.max}], [${dy.min},${dy.max}]): $x, $y")
    Z2(dx.normalize(x), dy.normalize(y)).z
  }

  override def invert(i: Long): (Double, Double) = {
    val (x, y) = Z2(i).decode
    (dx.denormalize(x), dy.denormalize(y))
  }

  override def ranges(xy: Seq[(Double, Double, Double, Double)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    val zbounds = xy.map { case (xmin, ymin, xmax, ymax) => ZRange(index(xmin, ymin), index(xmax, ymax)) }
    Z2.zranges(zbounds.toArray, precision, maxRanges)
  }
}

object Z2SFC extends Z2SFC(31)
