/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.stats.MinMaxHelper._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.reflect.ClassTag

/**
 * The MinMax stat merely returns the min/max of an attribute's values.
 * Works with dates, integers, longs, doubles, and floats.
 *
 * @param attribute attribute index for the attribute the histogram is being made for
 * @tparam T the type of the attribute the stat is targeting (needs to be comparable)
 */
class MinMax[T](val attribute: Int)(implicit defaults: MinMaxDefaults[T], ct: ClassTag[T]) extends Stat {

  override type S = MinMax[T]

  private var minValue: T = defaults.min
  private var maxValue: T = defaults.max

  private lazy val stringify = Stat.stringifier(ct.runtimeClass, json = true)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      updateMin(value)
      updateMax(value)
    }
  }

  override def +=(other: MinMax[T]): MinMax[T] = {
    updateMin(other.minValue)
    updateMax(other.maxValue)
    this
  }

  def min: T = if (minValue == defaults.min) null.asInstanceOf[T] else minValue
  def max: T = if (maxValue == defaults.max) null.asInstanceOf[T] else maxValue

  private [stats] def updateMin(sfval: T): Unit = {
    if (defaults.compare(minValue, sfval) > 0) {
      minValue = sfval
    }
  }

  private [stats] def updateMax(sfval: T): Unit = {
    if (defaults.compare(maxValue, sfval) < 0) {
      maxValue = sfval
    }
  }

  override def toJson(): String = s"""{ "min": ${stringify(min)}, "max": ${stringify(max)} }"""

  override def clear(): Unit = {
    minValue = defaults.min
    maxValue = defaults.max
  }
}

object MinMaxHelper {

  trait MinMaxDefaults[T] {
    def min: T
    def max: T
    def compare(l: T, r: T): Int
  }

  abstract class ComparableMinMax[T <: Comparable[T]] extends MinMaxDefaults[T] {
    override def compare(l: T, r: T): Int = l.compareTo(r)
  }

  implicit object MinMaxString extends ComparableMinMax[String] {
    override val min: String = "~~"
    override val max: String = ""
  }

  implicit object MinMaxInt extends ComparableMinMax[java.lang.Integer] {
    override val min: java.lang.Integer = java.lang.Integer.MAX_VALUE
    override val max: java.lang.Integer = java.lang.Integer.MIN_VALUE
  }

  implicit object MinMaxLong extends ComparableMinMax[java.lang.Long] {
    override val min: java.lang.Long = java.lang.Long.MAX_VALUE
    override val max: java.lang.Long = java.lang.Long.MIN_VALUE
  }

  implicit object MinMaxFloat extends ComparableMinMax[java.lang.Float] {
    override val min: java.lang.Float = java.lang.Float.MAX_VALUE
    override val max: java.lang.Float = java.lang.Float.MIN_VALUE
  }

  implicit object MinMaxDouble extends ComparableMinMax[java.lang.Double] {
    override val min: java.lang.Double = java.lang.Double.MAX_VALUE
    override val max: java.lang.Double = java.lang.Double.MIN_VALUE
  }

  implicit object MinMaxDate extends ComparableMinMax[Date] {
    override val min: Date = new Date(java.lang.Long.MAX_VALUE)
    override val max: Date = new Date(java.lang.Long.MIN_VALUE)
  }

  implicit object MinMaxGeometry extends MinMaxDefaults[Geometry] {
    override val min: Geometry = WKTUtils.read("POINT(180 90)")   // geohash zzz
    override val max: Geometry = WKTUtils.read("POINT(-180 -90)") // geohash 000
    override def compare(l: Geometry, r: Geometry): Int = Stat.getGeoHash(l).compareTo(Stat.getGeoHash(r))
  }
}