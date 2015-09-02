/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

trait SpaceFillingCurve[T] {
  def index(x: Double, y: Double): T
  def invert(i: T): (Double, Double)
  def ranges(x: (Double, Double), y: (Double, Double), precision: Int): Seq[(Long, Long)]
}

object Z2SFC extends SpaceFillingCurve[Z2] {

  private[curve] val xprec: Long = math.pow(2, 30).toLong - 1
  private[curve] val yprec: Long = math.pow(2, 30).toLong - 1

  val lon = NormalizedLon(xprec)
  val lat = NormalizedLat(yprec)

  override def index(x: Double, y: Double): Z2 = Z2(lon.normalize(x), lat.normalize(y))

  override def ranges(x: (Double, Double), y: (Double, Double), precision: Int = 64): Seq[(Long, Long)] =
    ZRange.zranges(index(x._1, y._1), index(x._2, y._2), Z2, precision)

  override def invert(z: Z2): (Double, Double) = {
    val (x, y) = z.decode
    (lon.denormalize(x), lat.denormalize(y))
  }
}
