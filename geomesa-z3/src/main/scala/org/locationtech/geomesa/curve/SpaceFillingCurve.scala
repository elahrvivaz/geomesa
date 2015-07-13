/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

trait SpaceFillingCurve[T] extends HasNormalizedLatLon {
  def index(x: Double, y: Double): T
  def invert(i: T): (Double, Double)
  def ranges(x: (Double, Double), y: (Double, Double)): Seq[(Long, Long)]
}

class Z2SFC extends SpaceFillingCurve[Z2] {

  override val xprec: Long = math.pow(2, 21).toLong - 1
  override val yprec: Long = math.pow(2, 21).toLong - 1

  override def index(x: Double, y: Double): Z2 =
    Z2(lon.normalize(x), lat.normalize(y))

  override def ranges(x: (Double, Double), y: (Double, Double)): Seq[(Long, Long)] =
    Z2.zranges(index(x._1, y._1), index(x._2, y._2))

  override def invert(z: Z2): (Double, Double, Long) = {
    val (x, y) = z.decode
    (lon.denormalize(x), lat.denormalize(y))
  }
}
