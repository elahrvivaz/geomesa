/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.hilbert

import com.google.common.base.Functions
import com.google.uzaygezen.core._
import com.google.uzaygezen.core.ranges.{LongRange, LongRangeHome}
import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon, NormalizedTime}
import org.locationtech.geomesa.curve.time.{BinnedTime, TimePeriod}
import org.locationtech.geomesa.curve.time.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{NormalizedDimension, SpaceFillingPointCurve3D}
import org.locationtech.sfcurve.IndexRange

class UzaygezenHilbert3SFC(period: TimePeriod, precision: Int = 20) extends SpaceFillingPointCurve3D {

  // restrict to 20 bits to allow use of Longs (max 62 bits)
  require(precision > 0 && precision < 21, "Precision (bits) per dimension must be in [1,20]")

  import scala.collection.JavaConversions._

  private val state = new ThreadLocal[(CompactHilbertCurve, Array[BitVector], BitVector)] {
    override def initialValue: (CompactHilbertCurve, Array[BitVector], BitVector) =
      (new CompactHilbertCurve(Array(precision, precision, precision)),
          Array.fill(3)(BitVectorFactories.OPTIMAL.apply(precision)),
          BitVectorFactories.OPTIMAL.apply(precision * 3))
  }

  override val dx: NormalizedDimension = NormalizedLon(precision)
  override val dy: NormalizedDimension = NormalizedLat(precision)
  override val dz: NormalizedDimension = NormalizedTime(precision, BinnedTime.maxOffset(period).toDouble)

  override def index(x: Double, y: Double, z: Double): Long = {
    require(x >= dx.min && x <= dx.max && y >= dy.min && y <= dy.max,
      s"Value(s) out of bounds ([${dx.min},${dx.max}], [${dy.min},${dy.max}]): $x, $y")
    val (hilbert, p, chi) = state.get
    p(0).copyFrom(dx.normalize(x))
    p(1).copyFrom(dy.normalize(y))
    p(2).copyFrom(dz.normalize(z))
    hilbert.index(p, 0, chi)
    chi.toLong
  }

  override def invert(i: Long): (Double, Double, Double) = {
    val (hilbert, p, chi) = state.get
    hilbert.indexInverse(chi, p)
    (dx.denormalize(p(0).toLong.toInt), dy.denormalize(p(1).toLong.toInt), dz.denormalize(p(2).toLong.toInt))
  }

  override def ranges(xyz: Seq[(Double, Double, Double, Double, Double, Double)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    val query = xyz.map { case (xmin, ymin, zmin, xmax, ymax, zmax) =>
      val query = new java.util.ArrayList[LongRange](3)
      query.add(LongRange.of(dx.normalize(xmin).toLong, dx.normalize(xmax).toLong + 1L))
      query.add(LongRange.of(dy.normalize(ymin).toLong, dy.normalize(ymax).toLong + 1L))
      query.add(LongRange.of(dz.normalize(zmin).toLong, dz.normalize(zmax).toLong + 1L))
      query
    }

    val zero = new LongContent(0L)
    val one = new LongContent(1L)

    // TODO precision

    val filter: com.google.common.base.Function[LongRange, LongRange] = Functions.identity()
    val regionInspector:  RegionInspector[LongRange, LongContent] =
      SimpleRegionInspector.create[LongRange, java.lang.Long, LongContent, LongRange](query, one, filter, LongRangeHome.INSTANCE, zero)

    // PlainFilterCombiner since we're not using sub-ranges here
    val combiner = new PlainFilterCombiner[LongRange, java.lang.Long, LongContent, LongRange](LongRange.of(0, 1))

    val max = maxRanges.getOrElse(Int.MaxValue)

    val queryBuilder: QueryBuilder[LongRange, LongRange] = BacktrackingQueryBuilder.create(
      regionInspector, combiner, max, true, LongRangeHome.INSTANCE, zero)

    val (hilbert, _, _) = state.get

    hilbert.accept(new ZoomingSpaceVisitorAdapter(hilbert, queryBuilder))

    queryBuilder.get().getFilteredIndexRanges.map { range =>
      // TODO validate ranges are actually in expected range?
      // TODO range is inclusive on both ends, is that expected in IndexRange?
      IndexRange(range.getIndexRange.getStart, range.getIndexRange.getEnd - 1, !range.isPotentialOverSelectivity)
    }
  }
}

object UzaygezenHilbert3SFC {

  private val SfcDay   = new UzaygezenHilbert3SFC(TimePeriod.Day)
  private val SfcWeek  = new UzaygezenHilbert3SFC(TimePeriod.Week)
  private val SfcMonth = new UzaygezenHilbert3SFC(TimePeriod.Month)
  private val SfcYear  = new UzaygezenHilbert3SFC(TimePeriod.Year)

  def apply(period: TimePeriod): UzaygezenHilbert3SFC = period match {
    case TimePeriod.Day   => SfcDay
    case TimePeriod.Week  => SfcWeek
    case TimePeriod.Month => SfcMonth
    case TimePeriod.Year  => SfcYear
  }
}
