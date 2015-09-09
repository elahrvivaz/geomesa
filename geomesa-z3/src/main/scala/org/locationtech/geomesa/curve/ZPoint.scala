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

  def split(value: Long): Long
  def combine(z: Long): Int

  // the number of child regions, e.g. for 2 dims it would be 00 01 10 11
  lazy val subRegions = math.pow(2, dims).toInt
}

object ZN {
  def apply(dims: Int) = if (dims == 2) Z2 else if (dims == 3) Z3 else throw new NotImplementedError()
  def toString(z: Long) = (Array.fill(64)("0") ++ z.toBinaryString).takeRight(64).mkString("")
}

trait ZPoint extends Any {
  def z: Long
  def dims: Int
  def dim(i: Int): Int
  def decode: Product
  def bitsToString = f"(${z.toBinaryString.toLong}%016d)" +
      s"(${(0 until dims).map(dim).map(_.toBinaryString.toLong).map(_.formatted("%08d")).mkString(",")})"
}
