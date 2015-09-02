/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

trait ZN {
  def dims: Int
  def bits: Int
  def maxValue: Long
  def apply(z: Long): ZPoint
  def apply(dims: Int*): ZPoint
}

object ZN {
  def apply(dims: Int) = if (dims == 2) Z2 else if (dims == 3) Z3 else throw new NotImplementedError()
}

trait ZPoint extends Any {
  def z: Long
  def dims: Int
  def dim(i: Int): Int
  def decode: Product
  def bitsToString = f"(${z.toBinaryString.toLong}%016d)" +
      s"(${(0 until dims).map(dim).map(_.toBinaryString.toLong).map(_.formatted("%08d")).mkString(",")})"
}
