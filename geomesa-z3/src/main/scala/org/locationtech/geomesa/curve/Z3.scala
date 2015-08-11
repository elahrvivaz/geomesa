/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.curve

class Z3(val z: Long) extends AnyVal with ZPoint[(Int, Int, Int)] {
  override def dims = Z3.MAX_DIM
  override def bits = Z3.TOTAL_BITS
  override def decode = (dim(0), dim(1), dim(2))
  override def dim(i: Int): Int = if (i == 0) Z3.combine(z) else Z3.combine(z >> i)
  override def toString = f"$z $decode"
}

object Z3 {

  final val MAX_DIM = 3
  final val MAX_BITS = 21
  final val MAX_MASK = 0x1fffffL
  final val TOTAL_BITS = MAX_BITS * MAX_DIM

  def apply(zvalue: Long) = new Z3(zvalue)
  def apply(x: Int, y: Int, t: Int) = new Z3(split(x) | split(y) << 1 | split(t) << 2)
  def unapply(z: Z3): Option[(Int, Int, Int)] = Some(z.decode)

  /** insert 00 between every bit in value. Only first 21 bits can be considered. */
  def split(value: Long): Long = {
    var x = value & MAX_MASK
    x = (x | x << 32) & 0x1f00000000ffffL
    x = (x | x << 16) & 0x1f0000ff0000ffL
    x = (x | x << 8)  & 0x100f00f00f00f00fL
    x = (x | x << 4)  & 0x10c30c30c30c30c3L
    (x | x << 2)      & 0x1249249249249249L
  }

  /** combine every third bit to form a value. Maximum value is 21 bits. */
  def combine(z: Long): Int = {
    var x = z & 0x1249249249249249L
    x = (x ^ (x >>  2)) & 0x10c30c30c30c30c3L
    x = (x ^ (x >>  4)) & 0x100f00f00f00f00fL
    x = (x ^ (x >>  8)) & 0x1f0000ff0000ffL
    x = (x ^ (x >> 16)) & 0x1f00000000ffffL
    x = (x ^ (x >> 32)) & MAX_MASK
    x.toInt
  }
}
