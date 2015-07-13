/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.curve

import org.joda.time.Weeks

trait SpaceTimeFillingCurve[T] extends HasNormalizedLatLon with HasNormalizedTime {
  def index(x: Double, y: Double, t: Long): T
  def invert(i: T): (Double, Double, Long)
  def ranges(x: (Double, Double), y: (Double, Double), t: (Long, Long)): Seq[(Long, Long)]
}

class Z3SFC extends SpaceTimeFillingCurve[Z3] {

  override val xprec: Long = math.pow(2, 21).toLong - 1
  override val yprec: Long = math.pow(2, 21).toLong - 1
  override val tprec: Long = math.pow(2, 20).toLong - 1
  override val tmax: Double = Weeks.weeks(1).toStandardSeconds.getSeconds.toDouble

  override def index(x: Double, y: Double, t: Long): Z3 =
    Z3(lon.normalize(x), lat.normalize(y), time.normalize(t))

  override def ranges(x: (Double, Double), y: (Double, Double), t: (Long, Long)): Seq[(Long, Long)] =
    Z3.zranges(index(x._1, y._1, t._1), index(x._2, y._2, t._2))

  override def invert(z: Z3): (Double, Double, Long) = {
    val (x, y, t) = z.decode
    (lon.denormalize(x), lat.denormalize(y), time.denormalize(t).toLong)
  }
}
