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
import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.locationtech.geomesa.curve.{NormalizedDimension, SpaceFillingPointCurve2D}
import org.locationtech.sfcurve.IndexRange

class UzaygezenHilbert2SFC(precision: Int = 31) extends SpaceFillingPointCurve2D {

  require(precision > 0 && precision < 32, "Precision (bits) per dimension must be in [1,31]")

  import scala.collection.JavaConversions._

  private val state = new ThreadLocal[(CompactHilbertCurve, Array[BitVector], BitVector)] {
    override def initialValue: (CompactHilbertCurve, Array[BitVector], BitVector) =
      (new CompactHilbertCurve(Array(precision, precision)),
          Array.fill(2)(BitVectorFactories.OPTIMAL.apply(precision)),
          BitVectorFactories.OPTIMAL.apply(precision * 2))
  }

  override val dx: NormalizedDimension = NormalizedLon(precision)
  override val dy: NormalizedDimension = NormalizedLat(precision)

  override def index(x: Double, y: Double): Long = {
    require(x >= dx.min && x <= dx.max && y >= dy.min && y <= dy.max,
      s"Value(s) out of bounds ([${dx.min},${dx.max}], [${dy.min},${dy.max}]): $x, $y")
    val (hilbert, p, chi) = state.get
    p(0).copyFrom(dx.normalize(x))
    p(1).copyFrom(dy.normalize(y))
    hilbert.index(p, 0, chi)
    chi.toLong
  }

  override def invert(i: Long): (Double, Double) = {
    val (hilbert, p, chi) = state.get
    hilbert.indexInverse(chi, p)
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
      query.add(LongRange.of(dx.normalize(xmin).toLong, dx.normalize(xmax).toLong + 1L))
      query.add(LongRange.of(dy.normalize(ymin).toLong, dy.normalize(ymax).toLong + 1L))
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

object UzaygezenHilbert2SFC extends UzaygezenHilbert2SFC
