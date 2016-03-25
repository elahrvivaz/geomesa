/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.lang.StringEscapeUtils
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.text.{EnhancedTokenParsers, WKTUtils}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.reflect.ClassTag
import scala.util.parsing.combinator.RegexParsers

/**
 * Stats used by the StatsIterator to compute various statistics server-side for a given query.
 */
trait Stat {

  type S <: Stat

  /**
   * Compute statistics based upon the given simple feature.
   * This method will be called for every SimpleFeature a query returns.
   *
   * @param sf feature to evaluate
   */
  def observe(sf: SimpleFeature): Unit

  /**
   * Add another stat to this stat. Avoids allocating another object.
   *
   * @param other the other stat to add
   */
  def +=(other: S): Unit

  /**
   * Non type-safe add - if stats are not the same type, will throw an exception
   *
   * @param other the other stat to add
   */
  def +=(other: Stat)(implicit d: DummyImplicit): Unit = this += other.asInstanceOf[S]

  /**
    * Combine two stats into a new stat
    *
    * @param other the other stat to add
    */
  def +(other: S): S

  /**
    * Non type-safe add - if stats are not the same type, will throw an exception
    *
    * @param other the other stat to add
    */
  def +(other: Stat)(implicit d: DummyImplicit): Stat = this + other.asInstanceOf[S]

  /**
   * Serves as serialization needed for storing the computed statistic in a SimpleFeature.
   *
   * @return stat serialized as a json string
   */
  // noinspection AccessorLikeMethodIsEmptyParen
  def toJson(): String

  /**
   * Necessary method used by the StatIterator. Indicates if the stat has any values or not
   *
   * @return true if stat contains values
   */
  def isEmpty: Boolean

  /**
   * Clears the stat to its original state when first initialized.
   * Necessary method used by the StatIterator.
   */
  def clear(): Unit
}

/**
 * This class contains parsers which dictate how to instantiate a particular Stat.
 * Stats are created by passing a stats string as a query hint (QueryHints.STATS_STRING).
 *
 * A valid stats string should adhere to the parsers here:
 * e.g. "MinMax(attributeName);IteratorCount" or "RangeHistogram(attributeName,10,0,100)"
 * (see tests for more use cases)
 */
object Stat {

  def apply(sft: SimpleFeatureType, s: String) = new StatParser(sft).parse(s)

  def Count(ecql: Filter): String = Count(ECQL.toCQL(ecql))
  def Count(ecql: String): String = s"Count(${safeString(ecql)})"
  def MinMax(attribute: String): String = s"MinMax(${safeString(attribute)})"
  def EnumeratedHistogram(attribute: String): String = s"EnumeratedHistogram(${safeString(attribute)})"
  def RangeHistogram[T](attribute: String, bins: Int, min: T, max: T)(implicit ct: ClassTag[T]): String = {
    val stringify = stringifier(ct.runtimeClass)
    s"RangeHistogram(${safeString(attribute)},$bins,${safeString(stringify(min))},${safeString(stringify(max))})"
  }
  def IteratorStackCounter(): String = "IteratorStackCounter"
  def SeqStat(stats: Seq[String]): String = stats.mkString(";")

  private def safeString(s: String): String = s""""${StringEscapeUtils.escapeJava(s)}"""" // there's extra quotes here

  def getGeoHash(value: Geometry, length: Int = 2): Int = {
    val centroid = value.getCentroid
    GeoHash(centroid.getX, centroid.getY, 5 * length).toInt
  }

  def stringifier[T](clas: Class[T], json: Boolean = false): Any => String = {
    val toString: (Any) => String = if (classOf[Geometry].isAssignableFrom(clas)) {
      (v) => WKTUtils.write(v.asInstanceOf[Geometry])
    } else if (clas == classOf[Date]) {
      (v) => GeoToolsDateFormat.print(v.asInstanceOf[Date].getTime)
    } else {
      (v) => v.toString
    }

    // add quotes to json strings if needed
    if (json && !classOf[Number].isAssignableFrom(clas)) {
      (v) => if (v == null) "null" else s""""${toString(v)}""""
    } else {
      (v) => if (v == null) "null" else toString(v)
    }
  }

  def destringifier[T](clas: Class[T]): String => T =
    if (clas == classOf[String]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.asInstanceOf[T]
    } else if (clas == classOf[Integer]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toInt.asInstanceOf[T]
    } else if (clas == classOf[jLong]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toLong.asInstanceOf[T]
    } else if (clas == classOf[jFloat]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toFloat.asInstanceOf[T]
    } else if (clas == classOf[jDouble]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toDouble.asInstanceOf[T]
    } else if (classOf[Geometry].isAssignableFrom(clas)) {
      (s) => if (s == "null") null.asInstanceOf[T] else WKTUtils.read(s).asInstanceOf[T]
    } else if (clas == classOf[Date]) {
      (s) => if (s == "null") null.asInstanceOf[T] else GeoToolsDateFormat.parseDateTime(s).toDate.asInstanceOf[T]
    } else {
      throw new RuntimeException(s"Unexpected class binding for stat attribute: $clas")
    }

  class StatParser(sft: SimpleFeatureType) extends RegexParsers with EnhancedTokenParsers {

    /**
     * Obtains the index of the attribute within the SFT
     *
     * @param attribute attribute name as a string
     * @return attribute index
     */
    private def getAttrIndex(attribute: String): Int = {
      val i = sft.indexOf(attribute)
      require(i != -1, s"Attribute '$attribute' does not exist in sft ${DataUtilities.encodeType(sft)}")
      i
    }

    val numBinRegex = """[1-9][0-9]*""".r // any non-zero positive int
    val argument = dequotedString | "[a-zA-Z0-9_]+".r

    def countParser: Parser[CountStat] = {
      "Count(" ~> argument <~ ")" ^^ { cql => new CountStat(cql) }
    }

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> argument <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getType(attribute).getBinding

          if (attrType == classOf[String]) {
            new MinMax[String](attrIndex)
          } else if (attrType == classOf[Date]) {
            new MinMax[Date](attrIndex)
          } else if (attrType == classOf[jLong]) {
            new MinMax[jLong](attrIndex)
          } else if (attrType == classOf[Integer]) {
            new MinMax[Integer](attrIndex)
          } else if (attrType == classOf[jDouble]) {
            new MinMax[jDouble](attrIndex)
          } else if (attrType == classOf[jFloat]) {
            new MinMax[jFloat](attrIndex)
          } else if (classOf[Geometry].isAssignableFrom(attrType)) {
            new MinMax[Geometry](attrIndex)
          } else {
            println(s"\n\nCannot create stat for invalid type: $attrType for attribute: $attribute\n\n")
            throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorStackCounter" ^^ { case _ => new IteratorStackCounter() }
    }

    def enumeratedHistogramParser: Parser[EnumeratedHistogram[_]] = {
      "EnumeratedHistogram(" ~> argument <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getType(attribute).getBinding

          if (attrType == classOf[String]) {
            new EnumeratedHistogram[String](attrIndex)
          } else if (attrType == classOf[Date]) {
            new EnumeratedHistogram[Date](attrIndex)
          } else if (attrType == classOf[Integer]) {
            new EnumeratedHistogram[Integer](attrIndex)
          } else if (attrType == classOf[jLong]) {
            new EnumeratedHistogram[jLong](attrIndex)
          } else if (attrType == classOf[jFloat]) {
            new EnumeratedHistogram[jFloat](attrIndex)
          } else if (attrType == classOf[jDouble]) {
            new EnumeratedHistogram[jDouble](attrIndex)
          } else if (classOf[Geometry].isAssignableFrom(attrType )) {
            new EnumeratedHistogram[Geometry](attrIndex)
          } else {
            throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def rangeHistogramParser: Parser[RangeHistogram[_]] = {
      "RangeHistogram(" ~> argument ~ "," ~ numBinRegex ~ "," ~ argument ~ "," ~ argument <~ ")" ^^ {
        case attribute ~ "," ~ numBins ~ "," ~ lowerEndpoint ~ "," ~ upperEndpoint =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getType(attribute).getBinding

          if (attrType == classOf[String]) {
            new RangeHistogram[String](attrIndex, numBins.toInt, (lowerEndpoint, upperEndpoint))
          } else if (attrType == classOf[Date]) {
            val lower = GeoToolsDateFormat.parseDateTime(lowerEndpoint).toDate
            val upper = GeoToolsDateFormat.parseDateTime(upperEndpoint).toDate
            new RangeHistogram[Date](attrIndex, numBins.toInt, (lower, upper))
          } else if (attrType == classOf[Integer]) {
            val lower = lowerEndpoint.toInt
            val upper = upperEndpoint.toInt
            new RangeHistogram[Integer](attrIndex, numBins.toInt, (lower, upper))
          } else if (attrType == classOf[jLong]) {
            val lower = lowerEndpoint.toLong
            val upper = upperEndpoint.toLong
            new RangeHistogram[jLong](attrIndex, numBins.toInt, (lower, upper))
          } else if (attrType == classOf[jDouble]) {
            val lower = lowerEndpoint.toDouble
            val upper = upperEndpoint.toDouble
            new RangeHistogram[jDouble](attrIndex, numBins.toInt, (lower, upper))
          } else if (attrType == classOf[jFloat]) {
            val lower = lowerEndpoint.toFloat
            val upper = upperEndpoint.toFloat
            new RangeHistogram[jFloat](attrIndex, numBins.toInt, (lower, upper))
          } else if (classOf[Geometry].isAssignableFrom(attrType )) {
            val lower = WKTUtils.read(lowerEndpoint)
            val upper = WKTUtils.read(upperEndpoint)
            new RangeHistogram[Geometry](attrIndex, numBins.toInt, (lower, upper))
          } else {
            throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def statParser: Parser[Stat] =
      countParser | minMaxParser | iteratorStackParser | enumeratedHistogramParser | rangeHistogramParser

    def statsParser: Parser[Stat] = {
      rep1sep(statParser, ";") ^^ {
        case statParsers: Seq[Stat] => if (statParsers.length == 1) statParsers.head else new SeqStat(statParsers)
      }
    }

    def parse(s: String): Stat = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse the stats string: $s\n${failure.msg}")
      }
    }
  }
}
