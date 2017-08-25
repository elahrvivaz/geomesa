/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.hilbert.impl

import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.curve.hilbert.impl.SpaceFillingCurve.{Composable, OrdinalNumber}

import scala.reflect._

object Dimensions {
  trait DimensionLike[T] {
    def eq(a: T, b: T): Boolean
    def lt(a: T, b: T): Boolean
    def lteq(a: T, b: T): Boolean = !gt(a, b)
    def gt(a: T, b: T): Boolean
    def gteq(a: T, b: T): Boolean = !lt(a, b)
    def compare(a: T, b: T): Int =
      if (lt(a, b)) -1
      else {
        if (gt(a, b)) 1
        else 0
      }
    def add(a: T, b: T): T
    def subtract(a: T, b: T): T
    def multiply(a: T, b: T): T
    def divide(a: T, b: T): T
    def toDouble(a: T): Double
    def min(a: T, b: T): T =
      if (gt(a, b)) b
      else a
    def floor(a: Double): T
    def fromDouble(a: Double): T
  }

  implicit object DimensionLikeDouble extends DimensionLike[Double] {
    val maxTolerance = 1e-10
    def eq(a: Double, b: Double) = Math.abs(a - b) <= maxTolerance
    def lt(a: Double, b: Double) = (b - a) > maxTolerance
    def gt(a: Double, b: Double) = (a - b) > maxTolerance
    def add(a: Double, b: Double) = a + b
    def subtract(a: Double, b: Double) = a - b
    def multiply(a: Double, b: Double) = a * b
    def divide(a: Double, b: Double) = a / b
    def toDouble(a: Double) = a
    def floor(a: Double) = Math.floor(a)
    def fromDouble(a: Double) = a
  }

  implicit object DimensionLikeLong extends DimensionLike[Long] {
    def eq(a: Long, b: Long) = a == b
    def lt(a: Long, b: Long) = a < b
    def gt(a: Long, b: Long) = a > b
    def add(a: Long, b: Long) = a + b
    def subtract(a: Long, b: Long) = a - b
    def multiply(a: Long, b: Long) = a * b
    def divide(a: Long, b: Long) = a / b
    def toDouble(a: Long) = a.toDouble
    def floor(a: Double) = Math.floor(a).toLong
    def fromDouble(a: Double) = Math.round(a).toLong
  }

  implicit object DimensionLikeDate extends DimensionLike[DateTime] {
    def eq(a: DateTime, b: DateTime) = a == b
    def lt(a: DateTime, b: DateTime) = a.getMillis < b.getMillis
    def gt(a: DateTime, b: DateTime) = a.getMillis > b.getMillis
    def add(a: DateTime, b: DateTime) = new DateTime(a.getMillis + b.getMillis, DateTimeZone.forID("UTC"))
    def subtract(a: DateTime, b: DateTime) = new DateTime(a.getMillis - b.getMillis, DateTimeZone.forID("UTC"))
    def multiply(a: DateTime, b: DateTime) = new DateTime(a.getMillis * b.getMillis, DateTimeZone.forID("UTC"))
    def divide(a: DateTime, b: DateTime) = new DateTime(a.getMillis / b.getMillis, DateTimeZone.forID("UTC"))
    def toDouble(a: DateTime) = a.getMillis.toDouble
    def floor(a: Double) = new DateTime(Math.floor(a).toLong, DateTimeZone.forID("UTC"))
    def fromDouble(a: Double) = new DateTime(a.toLong, DateTimeZone.forID("UTC"))
  }
}

import org.locationtech.geomesa.curve.hilbert.impl.Dimensions._

trait BareDimension[T] {
  def index(value: T): OrdinalNumber
  def indexAny(value: Any): OrdinalNumber
  def inverseIndex(ordinal: OrdinalNumber): Dimension[T]
}

case class HalfDimension[T](extreme: T, isExtremeIncluded: Boolean)

// for now, assume all dimensions are bounded
// (reasonable, unless you're really going to use BigInt or
// some other arbitrary-precision class as the basis)
case class Dimension[T : DimensionLike](name: String, min: T, isMinIncluded: Boolean, max: T, isMaxIncluded: Boolean, precision: OrdinalNumber)(implicit classTag: ClassTag[T])
    extends Composable with BareDimension[T] {

  val basis = implicitly[DimensionLike[T]]

  val span = basis.subtract(max, min)
  val numBins = 1L << precision
  val penultimateBin = numBins - 1L

  val doubleSpan = basis.toDouble(span)
  val doubleNumBins = numBins.toDouble
  val doubleMin = basis.toDouble(min)
  val doubleMax = basis.toDouble(max)

  val doubleMid = 0.5 * (doubleMin + doubleMax)

  def containsAny(value: Any): Boolean = {
    val coercedValue: T = value.asInstanceOf[T]
    contains(coercedValue)
  }

  def contains(value: T): Boolean =
    (basis.gt(value, min) ||(basis.eq(value, min) && isMinIncluded)) &&
        (basis.lt(value, max) ||(basis.eq(value, max) && isMaxIncluded))

  def indexAny(value: Any): OrdinalNumber = {
    val coercedValue: T = value.asInstanceOf[T]
    index(coercedValue)
  }

  def index(value: T): OrdinalNumber = {
    val doubleValue = basis.toDouble(value)
    Math.min(
      Math.floor(doubleNumBins * (doubleValue - doubleMin) / doubleSpan),
      penultimateBin
    ).toLong
  }

  def getLowerBound(ordinal: OrdinalNumber): HalfDimension[T] = {
    /*
    val minimum = numeric.toDouble(dim.min) + numeric.toDouble(dim.span) * ordinal.toDouble / size.toDouble
    val incMin = dim.isMinIncluded || (ordinal == 0L)
     */
    val x = basis.fromDouble(doubleMin + doubleSpan * ordinal.toDouble / doubleNumBins)
    val included = isMinIncluded || (ordinal == 0L)
    HalfDimension[T](x, included)
  }

  def getUpperBound(ordinal: OrdinalNumber): HalfDimension[T] = {
    /*
    val max = numeric.toDouble(dim.min) + numeric.toDouble(dim.span) * (1L + ordinal.toDouble) / size.toDouble
    val incMax = dim.isMaxIncluded && (ordinal == (size - 1L))
     */
    val x = basis.fromDouble(doubleMin + doubleSpan * (1L + ordinal.toDouble) / doubleNumBins)
    val included = isMinIncluded || (ordinal == 0L)
    HalfDimension[T](x, included)
  }

  def inverseIndex(ordinal: OrdinalNumber): Dimension[T] = {
    val lowerBound = getLowerBound(ordinal)
    val upperBound = getUpperBound(ordinal)
    new Dimension[T](name, lowerBound.extreme, lowerBound.isExtremeIncluded, upperBound.extreme, upperBound.isExtremeIncluded, 1L)
  }

  override def toString: String =
    (if (isMinIncluded) "[" else "(") +
        min.toString + ", " + max.toString +
        (if (isMaxIncluded) "]" else ")") +
        "@" + precision.toString
}

// meant to represent a contiguous subset of bits from a
// larger, parent dimension
class SubDimension[T](val name: String, encoder: T => OrdinalNumber, parentPrecision: OrdinalNumber, bitsToSkip: OrdinalNumber, val precision: OrdinalNumber)
    extends Composable with BareDimension[T] {

  val numBitsRightToLose = parentPrecision - bitsToSkip - precision

  val mask = (1L << precision) - 1L

  def index(value: T): OrdinalNumber =
    (encoder(value) >>> numBitsRightToLose) & mask

  def indexAny(value: Any): OrdinalNumber = {
    val coercedValue: T = value.asInstanceOf[T]
    index(coercedValue)
  }

  def inverseIndex(ordinal: OrdinalNumber): Dimension[T] =
    throw new UnsupportedOperationException("The 'inverseIndex' method is ill defined for a SubDimension.")
}

object DefaultDimensions {
  import Dimensions._

  val dimLongitude = Dimension[Double]("x", -180.0, isMinIncluded = true, +180.0, isMaxIncluded = true, 18L)
  val dimLatitude = Dimension[Double]("y", -90.0, isMinIncluded = true, +90.0, isMaxIncluded = true, 17L)

  val MinNearDate = new DateTime(1900,  1,  1,  0,  0,  0, DateTimeZone.forID("UTC"))
  val MaxNearDate = new DateTime(2100, 12, 31, 23, 59, 59, DateTimeZone.forID("UTC"))
  val dimNearTime = Dimension("t", MinNearDate, isMinIncluded = true, MaxNearDate, isMaxIncluded = true, 15L)

  val MinYYYYDate = new DateTime(   0,  1,  1,  0,  0,  0, DateTimeZone.forID("UTC"))
  val MaxYYYYDate = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.forID("UTC"))
  val dimYYYYTime = Dimension("t", MinYYYYDate, isMinIncluded = true, MaxYYYYDate, isMaxIncluded = true, 20L)

  val MinDate = new DateTime(Long.MinValue >> 2L, DateTimeZone.forID("UTC"))
  val MaxDate = new DateTime(Long.MaxValue >> 2L, DateTimeZone.forID("UTC"))
  val dimTime = Dimension("t", MinDate, isMinIncluded = true, MaxDate, isMaxIncluded = true, 60L)

  def createLongitude(atPrecision: OrdinalNumber): Dimension[Double] =
    dimLongitude.copy(precision = atPrecision)

  def createLatitude(atPrecision: OrdinalNumber): Dimension[Double] =
    dimLatitude.copy(precision = atPrecision)

  def createDateTime(atPrecision: OrdinalNumber): Dimension[DateTime] =
    dimTime.copy(precision = atPrecision)

  def createYYYYDateTime(atPrecision: OrdinalNumber): Dimension[DateTime] =
    dimYYYYTime.copy(precision = atPrecision)

  def createNearDateTime(atPrecision: OrdinalNumber): Dimension[DateTime] =
    dimNearTime.copy(precision = atPrecision)

  def createNearDateTime(minDate: DateTime, maxDate: DateTime, atPrecision: OrdinalNumber): Dimension[DateTime] =
    dimNearTime.copy(min = minDate, max = maxDate, precision = atPrecision)

  def createDimension[T](name: String, minimum: T, maximum: T, precision: OrdinalNumber)(implicit dimLike: DimensionLike[T], ctag: ClassTag[T]): Dimension[T] =
    Dimension[T](name, minimum, isMinIncluded = true, maximum, isMaxIncluded = true, precision)

  class IdentityDimension(name: String, precision: OrdinalNumber)
      extends Dimension[Long](name, 0L, isMinIncluded=true, (1L << precision) - 1L, isMaxIncluded=true, precision) {

    override def index(value: OrdinalNumber): OrdinalNumber = value

    override def indexAny(value: Any): OrdinalNumber = value.asInstanceOf[OrdinalNumber]

    override def inverseIndex(ordinal: OrdinalNumber): Dimension[Long] =
      Dimension[Long]("dummy", ordinal, isMinIncluded=true, ordinal, isMaxIncluded=true, 0)
  }

  def createIdentityDimension(name: String, precision: OrdinalNumber): Dimension[Long] =
    new IdentityDimension(name, precision)

  def createIdentityDimension(precision: OrdinalNumber): Dimension[Long] =
    createIdentityDimension(s"Identity_$precision", precision)
}
