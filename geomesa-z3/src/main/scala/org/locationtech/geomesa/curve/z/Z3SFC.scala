/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.z

import org.locationtech.geomesa.curve.NormalizedDimension._
import org.locationtech.geomesa.curve.time.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.time.{BinnedTime, TimePeriod}
import org.locationtech.geomesa.curve.{NormalizedDimension, SpaceFillingPointCurve3D}
import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.{Z3, ZRange}

/**
  * Z3 space filling curve
  *
  * @param period time period used to bin results
  * @param precision bits used per dimension - note all precisions must sum to less than 64
  */
class Z3SFC(period: TimePeriod, precision: Int = 21) extends SpaceFillingPointCurve3D {

  require(precision > 0 && precision < 22, "Precision (bits) per dimension must be in [1,21]")

  override val dx: NormalizedDimension = NormalizedLon(precision)
  override val dy: NormalizedDimension = NormalizedLat(precision)
  override val dz: NormalizedDimension = NormalizedTime(precision, BinnedTime.maxOffset(period).toDouble)

  override def index(x: Double, y: Double, z: Double): Long = {
    require(x >= dx.min && x <= dx.max && y >= dy.min && y <= dy.max && z >= dz.min && z <= dz.max,
      s"Value(s) out of bounds ([${dx.min},${dx.max}], [${dy.min},${dy.max}], [${dz.min},${dz.max}]): $x, $y, $z")
    Z3(dx.normalize(x), dy.normalize(y), dz.normalize(z)).z
  }

  override def invert(i: Long): (Double, Double, Double) = {
    val (x, y, z) = Z3(i).decode
    (dx.denormalize(x), dy.denormalize(y), dz.denormalize(z))
  }

  override def ranges(xyz: Seq[(Double, Double, Double, Double, Double, Double)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    val zbounds = xyz.map { case (xmin, ymin, zmin, xmax, ymax, zmax) =>
      ZRange(index(xmin, ymin, zmin), index(xmax, ymax, zmax))
    }
    Z3.zranges(zbounds.toArray, precision, maxRanges)
  }
}

object Z3SFC {

  private val SfcDay   = new Z3SFC(TimePeriod.Day)
  private val SfcWeek  = new Z3SFC(TimePeriod.Week)
  private val SfcMonth = new Z3SFC(TimePeriod.Month)
  private val SfcYear  = new Z3SFC(TimePeriod.Year)

  def apply(period: TimePeriod): Z3SFC = period match {
    case TimePeriod.Day   => SfcDay
    case TimePeriod.Week  => SfcWeek
    case TimePeriod.Month => SfcMonth
    case TimePeriod.Year  => SfcYear
  }
}