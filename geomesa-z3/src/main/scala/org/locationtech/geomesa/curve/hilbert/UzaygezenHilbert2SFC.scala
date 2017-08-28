/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.hilbert

import com.google.common.base.Functions
import com.google.uzaygezen.core.ranges.{LongRange, LongRangeHome}
import com.google.uzaygezen.core._
import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.locationtech.geomesa.curve.{NormalizedDimension, SpaceFillingPointCurve2D}
import org.locationtech.sfcurve.{CoveredRange, IndexRange, OverlappingRange}

class UzaygezenHilbert2SFC(precision: Int) extends SpaceFillingPointCurve2D {

  require(precision > 0 && precision < 32, "Precision (bits) per dimension must be in [1,31]")

  import scala.collection.JavaConversions._

  // TODO thread safe?
  private val hilbert = new ThreadLocal[CompactHilbertCurve]() {
    override def initialValue: CompactHilbertCurve = new CompactHilbertCurve(Array(precision, precision))
  }

  override val dx: NormalizedDimension = NormalizedLon(precision)
  override val dy: NormalizedDimension = NormalizedLat(precision)

  override def index(x: Double, y: Double): Long = {
    require(x >= dx.min && x <= dx.max && y >= dy.min && y <= dy.max,
      s"Value(s) out of bounds ([${dx.min},${dx.max}], [${dy.min},${dy.max}]): $x, $y")
    val p = Array.fill(2)(BitVectorFactories.OPTIMAL.apply(precision))
    p(0).copyFrom(dx.normalize(x))
    p(1).copyFrom(dy.normalize(y))
    val chi = BitVectorFactories.OPTIMAL.apply(precision * 2)
    hilbert.get.index(p, 0, chi)
    chi.toLong
  }

  override def invert(i: Long): (Double, Double) = {
    // TODO cache arrays
    val p = Array.fill(2)(BitVectorFactories.OPTIMAL.apply(precision))
    val chi = BitVectorFactories.OPTIMAL.apply(precision * 2)
    hilbert.get.indexInverse(chi, p)
    (dx.denormalize(p(0).toLong.toInt), dy.denormalize(p(1).toLong.toInt))
  }

  override def ranges(xy: Seq[(Double, Double, Double, Double)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    // TODO this might not work with 63 bits
    val query = xy.map { case (xmin, ymin, xmax, ymax) =>
      val query = new java.util.ArrayList[LongRange](2)
      println(s"$xmin/$xmax -> ${dx.normalize(xmin)}/${dx.normalize(xmax)}")
      println(s"$ymin/$ymax -> ${dy.normalize(ymin)}/${dy.normalize(ymax)}")
      query.add(LongRange.of(dx.normalize(xmin), dx.normalize(xmax))) // TODO + 1 ?
      query.add(LongRange.of(dy.normalize(ymin), dy.normalize(ymax)))
      query
    }

    val zero = new LongContent(0L)
    val one = new LongContent(1L)

    // TODO precision, maxRanges

    val filter = Functions.constant("").asInstanceOf[com.google.common.base.Function[LongRange, AnyRef]]
    val regionInspector:  RegionInspector[AnyRef, LongContent] =
      SimpleRegionInspector.create[AnyRef, java.lang.Long, LongContent, LongRange](query, one, filter, LongRangeHome.INSTANCE, zero)

    // PlainFilterCombiner since we're not using sub-ranges here
    val combiner: PlainFilterCombiner[AnyRef, java.lang.Long, LongContent, LongRange] = new PlainFilterCombiner(filter)

    val max = maxRanges.getOrElse(20) // TODO better default

    val queryBuilder: QueryBuilder[AnyRef, LongRange] = BacktrackingQueryBuilder.create(
      regionInspector, combiner, max, true, LongRangeHome.INSTANCE, zero)

    hilbert.get.accept(new ZoomingSpaceVisitorAdapter(hilbert.get, queryBuilder))

    queryBuilder.get().getFilteredIndexRanges.map { range =>
      if (range.isPotentialOverSelectivity) {
        // TODO validate ranges are actually in expected range?
        IndexRange(range.getIndexRange.getStart, range.getIndexRange.getEnd, contained = false)
      } else {
        IndexRange(range.getIndexRange.getStart, range.getIndexRange.getEnd, contained = true)
      }
    }
  }
}

object UzaygezenHilbert2SFC extends UzaygezenHilbert2SFC(31)
