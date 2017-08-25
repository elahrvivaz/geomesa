/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.hilbert

import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.locationtech.geomesa.curve.hilbert.impl.CompactHilbertCurve
import org.locationtech.geomesa.curve.hilbert.impl.SpaceFillingCurve.{OrdinalPair, OrdinalRanges, OrdinalVector, Query}
import org.locationtech.geomesa.curve.{NormalizedDimension, SpaceFillingPointCurve2D}
import org.locationtech.sfcurve.{CoveredRange, IndexRange}

class EichelbergerHilbert2SFC(precision: Int) extends SpaceFillingPointCurve2D {

  require(precision > 0 && precision < 32, "Precision (bits) per dimension must be in [1,31]")

  private val hilbert = CompactHilbertCurve(precision.toLong, precision.toLong)

  override val dx: NormalizedDimension = NormalizedLon(precision)
  override val dy: NormalizedDimension = NormalizedLat(precision)

  override def index(x: Double, y: Double): Long =
    // TODO hilbert.getOrComputeIndex(...)
    hilbert.index(OrdinalVector(dx.normalize(x).toLong, dy.normalize(y).toLong))

  override def invert(i: Long): (Double, Double) = {
    val OrdinalVector(x, y) = hilbert.inverseIndex(i)
    (dx.denormalize(x.toInt), dy.denormalize(y.toInt))
  }

  override def ranges(xy: Seq[(Double, Double, Double, Double)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    val query = xy.map { case (xmin, ymin, xmax, ymax) =>
      OrdinalRanges(
        OrdinalPair(dx.normalize(xmin).toLong, dx.normalize(xmax).toLong),
        OrdinalPair(dy.normalize(ymin).toLong, dy.normalize(ymax).toLong)
      )
    }
    // TODO precision, maxRanges, covered vs not
    hilbert.getRangesCoveringQuery(Query(query)).map { case OrdinalPair(lo, hi) => CoveredRange(lo, hi) }.toSeq
  }
}

object EichelbergerHilbert2SFC extends EichelbergerHilbert2SFC(31)
