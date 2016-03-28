/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util
import java.util.{Date, Locale}

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.reflect.ClassTag

abstract class BinnedArray[T](val length: Int, val bounds: (T, T)) {

  private[stats] val counts = Array.fill[Long](length)(0L)

  /**
   * Gets the count of entries in the given bin
   *
   * @param index bin index
   * @return count
   */
  def apply(index: Int): Long = counts(index)

  /**
   * Clears the counts
   */
  def clear(): Unit = {
    var i = 0
    while (i < length) {
      counts(i) = 0L
      i +=1
    }
  }

  /**
   * Increment the count for the bin corresponding to this value
   *
   * @param value value
   */
  def add(value: T): Unit = {
    val i = getIndex(value)
    if (i >= 0 && i < length) {
      counts(i) += 1
    }
  }

  /**
   * Add the results from another array to this one
   *
   * @param counts other array
   */
  def add(counts: Array[Long]): Unit = {
    var i = 0
    while (i < length) {
      this.counts(i) += counts(i)
      i += 1
    }
  }

  /**
   * Maps a value to a bin index.
   *
   * @param value value
   * @return bin index, or -1 if value is out of bounds
   */
  def getIndex(value: T): Int

  override def equals(other: Any): Boolean = other match {
    case that: BinnedArray[_] => bounds == that.bounds && java.util.Arrays.equals(counts, that.counts)
    case _ => false
  }

  override def hashCode(): Int = Seq(bounds, counts).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
}

object BinnedArray {
  def apply[T](length: Int, bounds: (T, T))(implicit c: ClassTag[T]): BinnedArray[T] = {
    val clas = c.runtimeClass
    if (clas == classOf[String]) {
      new BinnedStringArray(length, bounds.asInstanceOf[(String, String)]).asInstanceOf[BinnedArray[T]]
    } else if (clas == classOf[Integer]) {
      new BinnedIntegerArray(length, bounds.asInstanceOf[(Integer, Integer)]).asInstanceOf[BinnedArray[T]]
    } else if (clas == classOf[jLong]) {
      new BinnedLongArray(length, bounds.asInstanceOf[(jLong, jLong)]).asInstanceOf[BinnedArray[T]]
    } else if (clas == classOf[jFloat]) {
      new BinnedFloatArray(length, bounds.asInstanceOf[(jFloat, jFloat)]).asInstanceOf[BinnedArray[T]]
    } else if (clas == classOf[jDouble]) {
      new BinnedDoubleArray(length, bounds.asInstanceOf[(jDouble, jDouble)]).asInstanceOf[BinnedArray[T]]
    } else if (clas == classOf[Date]) {
      new BinnedDateArray(length, bounds.asInstanceOf[(Date, Date)]).asInstanceOf[BinnedArray[T]]
    } else if (classOf[Geometry].isAssignableFrom(clas)) {
      new BinnedGeometryArray(length, bounds.asInstanceOf[(Geometry, Geometry)]).asInstanceOf[BinnedArray[T]]
    } else {
      throw new UnsupportedOperationException(s"BinnedArray not implemented for ${clas.getName}")
    }
  }
}

class BinnedIntegerArray(length: Int, bounds: (Integer, Integer)) extends BinnedArray[Integer](length, bounds) {

  require(bounds._1 < bounds._2, s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = math.max(1, (bounds._2 - bounds._1).toFloat / length)

  override def getIndex(value: Integer): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      math.floor((value - bounds._1) / binSize).toInt
    }
  }
}

class BinnedLongArray(length: Int, bounds: (jLong, jLong)) extends BinnedArray[jLong](length, bounds) {

  require(bounds._1 < bounds._2, s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = math.max(1, (bounds._2 - bounds._1).toDouble / length)

  override def getIndex(value: jLong): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      math.floor((value - bounds._1) / binSize).toInt
    }
  }
}


class BinnedFloatArray(length: Int, bounds: (jFloat, jFloat)) extends BinnedArray[jFloat](length, bounds) {

  require(bounds._1 < bounds._2, s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = math.max(1, (bounds._2 - bounds._1) / length)

  override def getIndex(value: jFloat): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      math.floor((value - bounds._1) / binSize).toInt
    }
  }
}

class BinnedDoubleArray(length: Int, bounds: (jDouble, jDouble)) extends BinnedArray[jDouble](length, bounds) {

  require(bounds._1 < bounds._2, s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val binSize = math.max(1, (bounds._2 - bounds._1) / length)

  override def getIndex(value: jDouble): Int = {
    if (value < bounds._1 || value > bounds._2) { -1 } else {
      math.floor((value - bounds._1) / binSize).toInt
    }
  }
}

class BinnedStringArray(length: Int, bounds: (String, String)) extends BinnedArray[String](length, bounds) {

  val start = bounds._1.toLowerCase(Locale.US)
  val end   = bounds._2.toLowerCase(Locale.US)

  require(start.length == end.length, s"String bounds must be the same length: lower=${bounds._1} upper=${bounds._2}")
  require(start < end, s"Upper bound must be greater than lower bound: lower=${bounds._1} upper=${bounds._2}")

  private val firstDiff = start.zip(end).indexWhere { case (l, r) => l != r }

  def stringToLong(s: String): Long = {
    val sub = s.slice(firstDiff, bounds._1.length)
    var count = 0L
    var i = 0
    while (i < sub.length) {
      // we assume 36 values per char (a-z, 0-9) - this gives us a rough order of magnitude for binning
      count += sub(i).toInt * math.pow(36, sub.length - i - 1).toLong
      i += 1
    }
    count
  }

  private val min = stringToLong(start)
  private val max = stringToLong(end)

  private val binSize = math.max(1, (max - min) / length)

  override def getIndex(value: String): Int = {
    val lowerCaseValue = value.toLowerCase(Locale.US)
    if (lowerCaseValue < start || lowerCaseValue > end) { -1 } else {
      math.floor((stringToLong(lowerCaseValue) - min) / binSize).toInt
    }
  }
}

class BinnedDateArray(length: Int, bounds: (Date, Date)) extends BinnedArray[Date](length, bounds) {

  private val zero = bounds._1.getTime
  private val binSize = math.max(1, (bounds._2.getTime - zero).toFloat / length)

  require(zero < bounds._2.getTime, s"Upper bound must be after lower bound: lower=${bounds._1} upper=${bounds._2}")

  override def getIndex(value: Date): Int = math.floor((value.getTime - zero) / binSize).toInt
}

class BinnedGeometryArray(length: Int, bounds: (Geometry, Geometry)) extends BinnedArray[Geometry](length, bounds) {

  val zero = Stat.getGeoHash(bounds._1)
  val max  = Stat.getGeoHash(bounds._2)

  require(zero < max, s"GeoHashes aren't ordered: lower=${WKTUtils.write(bounds._1)}:$zero upper=${WKTUtils.write(bounds._2)}:$max")

  private val binSize = math.max(1, (max - zero).toFloat / length)

  override def getIndex(value: Geometry): Int = {
    val gh = Stat.getGeoHash(value)
    if (gh < zero || gh > max) { -1 } else {
      math.floor((gh - zero) / binSize).toInt
    }
  }
}