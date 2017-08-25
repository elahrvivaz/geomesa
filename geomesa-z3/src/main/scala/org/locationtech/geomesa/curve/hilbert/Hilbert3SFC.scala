/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.hilbert

import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon, NormalizedTime}
import org.locationtech.geomesa.curve.hilbert.impl.CompactHilbertCurve
import org.locationtech.geomesa.curve.hilbert.impl.SpaceFillingCurve.{OrdinalPair, OrdinalRanges, OrdinalVector, Query}
import org.locationtech.geomesa.curve.time.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.time.{BinnedTime, TimePeriod}
import org.locationtech.geomesa.curve.{NormalizedDimension, SpaceFillingPointCurve3D}
import org.locationtech.sfcurve.{CoveredRange, IndexRange}

class Hilbert3SFC(period: TimePeriod, precision: Int = 21) extends SpaceFillingPointCurve3D {

  require(precision > 0 && precision < 22, "Precision (bits) per dimension must be in [1,21]")

  private val hilbert = CompactHilbertCurve(precision.toLong, precision.toLong, precision.toLong)

  override val dx: NormalizedDimension = NormalizedLon(precision)
  override val dy: NormalizedDimension = NormalizedLat(precision)
  override val dz: NormalizedDimension = NormalizedTime(precision, BinnedTime.maxOffset(period).toDouble)

  override def index(x: Double, y: Double, z: Double): Long =
    // TODO hilbert.getOrComputeIndex(...)
    hilbert.index(OrdinalVector(dx.normalize(x).toLong, dy.normalize(y).toLong, dz.normalize(z).toLong))

  override def invert(i: Long): (Double, Double, Double) = {
    val OrdinalVector(x, y, z) = hilbert.inverseIndex(i)
    (dx.denormalize(x.toInt), dy.denormalize(y.toInt), dz.denormalize(z.toInt))
  }

  override def ranges(xyz: Seq[(Double, Double, Double, Double, Double, Double)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    val xranges = OrdinalRanges(xyz.map { case (xmin, _, _, xmax, _, _) =>
      OrdinalPair(dx.normalize(xmin).toLong, dx.normalize(xmax).toLong)
    }: _*)
    val yranges = OrdinalRanges(xyz.map { case (_, ymin, _, _, ymax, _) =>
      OrdinalPair(dy.normalize(ymin).toLong, dy.normalize(ymax).toLong)
    }: _*)
    val zranges = OrdinalRanges(xyz.map { case (_, _, zmin, _, _, zmax) =>
      OrdinalPair(dz.normalize(zmin).toLong, dz.normalize(zmax).toLong)
    }: _*)
    // TODO precision, maxRanges, covered vs not
    hilbert.getRangesCoveringQuery(Query(Seq(xranges, yranges, zranges))).map { case OrdinalPair(lo, hi) => CoveredRange(lo, hi) }.toSeq
  }
}

object Hilbert3SFC {

  private val SfcDay   = new Hilbert3SFC(TimePeriod.Day)
  private val SfcWeek  = new Hilbert3SFC(TimePeriod.Week)
  private val SfcMonth = new Hilbert3SFC(TimePeriod.Month)
  private val SfcYear  = new Hilbert3SFC(TimePeriod.Year)

  def apply(period: TimePeriod): Hilbert3SFC = period match {
    case TimePeriod.Day   => SfcDay
    case TimePeriod.Week  => SfcWeek
    case TimePeriod.Month => SfcMonth
    case TimePeriod.Year  => SfcYear
  }
}
