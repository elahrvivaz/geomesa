/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

trait ZPoint[T <: Product] extends Any {
  def z: Long
  def dims: Int
  def dim(i: Int): Int
  def decode: T

  def bitsToString = f"(${z.toBinaryString.toLong}%016d)" +
      s"(${(0 until dims).map(dim).map(_.toBinaryString.toLong).map(_.formatted("%08d")).mkString(",")})"
}

object ZPoint {
  def apply(z: Long, dims: Int): ZPoint[_] = {
    if (dims == 2) {
      new Z2(z)
    } else if (dims == 3) {
      new Z3(z)
    } else {
      throw new NotImplementedError()
    }
  }
}