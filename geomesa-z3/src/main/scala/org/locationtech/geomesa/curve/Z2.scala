/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */
package org.locationtech.geomesa.curve

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.curve.ZRange.ZPrefix

class Z2(val z: Long) extends AnyVal with ZPoint[(Int, Int)] {
  override def dims = Z2.MAX_DIM
  override def decode = (dim(0), dim(1))
  override def dim(i: Int) = if (i == 0) Z2.combine(z) else Z2.combine(z >> i)
  override def toString = f"$z $decode"
}

object Z2 {

  final val MAX_DIM = 2
  final val MAX_BITS = 30
  final val MAX_MASK = 0x3fffffffL

  def apply(z: Long) = new Z2(z)
  def apply(x: Int, y: Int): Z2 = new Z2(split(x) | split(y) << 1)
  def unapply(z: Z2): Option[(Int, Int)] = Some(z.decode)

  /** insert 0 between every bit in value. Only first 30 bits can be considered. */
  def split(value: Long): Long = {
    var x = value & MAX_MASK
    x = (x | x << 16) & 0x00003fff0000ffffL
    x = (x | x << 8)  & 0x003f00ff00ff00ffL
    x = (x | x << 4)  & 0x030f0f0f0f0f0f0fL
    x = (x | x << 2)  & 0x0333333333333333L
    (x | x << 1)      & 0x0555555555555555L
  }

  /** combine every second bit to form a value. Maximum value is 30 bits. */
  def combine(z: Long): Int = {
    var x = z & 0x0555555555555555L
    x = (x ^ (x >>  1)) & 0x0333333333333333L
    x = (x ^ (x >>  2)) & 0x030f0f0f0f0f0f0fL
    x = (x ^ (x >>  4)) & 0x003f00ff00ff00ffL
    x = (x ^ (x >>  8)) & 0x00003fff0000ffffL
    x = (x ^ (x >> 16)) & MAX_MASK
    x.toInt
  }

  def zBox(geom: Geometry): ZPrefix = {
    val env = geom.getEnvelopeInternal
    val ll = Z2SFC.index(env.getMinX, env.getMinY)
    val ur = Z2SFC.index(env.getMaxX, env.getMaxY)
    ZRange.longestCommonPrefix(ll.z, ur.z, MAX_DIM)
  }
}
