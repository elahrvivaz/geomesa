/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MinMaxTest extends Specification with StatTestHelper {

  def newStat[T](attribute: String, observe: Boolean = true): MinMax[T] = {
    val stat = Stat(sft, s"MinMax($attribute)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[MinMax[T]]
  }

  "MinMax stat" should {

    "work with dates" >> {
      "be empty initiallly" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        minMax.attribute mustEqual dateIndex
        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }

      "observe correct values" >> {
        val minMax = newStat[Date]("dtg")
        minMax.min mustEqual GeoToolsDateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
        minMax.max mustEqual GeoToolsDateFormat.parseDateTime("2012-01-01T23:00:00.000Z").toDate
      }

      "serialize to json" >> {
        val minMax = newStat[Date]("dtg")
        minMax.toJson() mustEqual """{ "min": "2012-01-01T00:00:00.000Z", "max": "2012-01-01T00:00:00.000Z" }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        minMax.toJson() mustEqual """{ "min": null, "max": null }"""
      }

      "serialize and deserialize" >> {
        val minMax = newStat[Date]("dtg")
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[Date]("dtg")
        val minMax2 = newStat[Date]("dtg", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.min mustEqual GeoToolsDateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate
        minMax2.max mustEqual GeoToolsDateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate

        minMax += minMax2

        minMax.min mustEqual GeoToolsDateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
        minMax.max mustEqual GeoToolsDateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate
        minMax2.min mustEqual GeoToolsDateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate
        minMax2.max mustEqual GeoToolsDateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate
      }

      "clear" >> {
        val minMax = newStat[Date]("dtg")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }
    }

    "work with ints" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        minMax.attribute mustEqual intIndex
        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.min mustEqual 0
        minMax.max mustEqual 99
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.toJson() mustEqual """{ "min": 0, "max": 99 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        minMax.toJson() mustEqual """{ "min": null, "max": null }"""
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        val minMax2 = newStat[java.lang.Integer]("intAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.min mustEqual 100
        minMax2.max mustEqual 199

        minMax += minMax2

        minMax.min mustEqual 0
        minMax.max mustEqual 199
        minMax2.min mustEqual 100
        minMax2.max mustEqual 199
      }

      "clear" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }
    }

//
//      "longs" in {
//        val stat = Stat(sft, "MinMax(longAttr)")
//        val minMax = stat.asInstanceOf[MinMax[java.lang.Long]]
//
//        minMax.attribute mustEqual longIndex
//        minMax.min mustEqual java.lang.Long.MAX_VALUE
//        minMax.max mustEqual java.lang.Long.MIN_VALUE
//        minMax.isEmpty must beFalse
//
//        features.foreach { stat.observe }
//
//        minMax.min mustEqual 0L
//        minMax.max mustEqual 99L
//
//        "serialize and deserialize" in {
//          val packed   = StatSerialization.pack(minMax, sft)
//          val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[MinMax[java.lang.Long]]
//
//          unpacked.toJson() mustEqual minMax.toJson()
//        }
//
//        "serialize and deserialize empty MinMax" in {
//          val stat = Stat(sft, "MinMax(longAttr)")
//          val minMax = stat.asInstanceOf[MinMax[java.lang.Long]]
//          val packed   = StatSerialization.pack(minMax, sft)
//          val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[MinMax[java.lang.Long]]
//
//          unpacked.toJson() mustEqual minMax.toJson()
//        }
//
//        "combine two MinMaxes" in {
//          val stat2 = Stat(sft, "MinMax(longAttr)")
//          val minMax2 = stat2.asInstanceOf[MinMax[java.lang.Long]]
//
//          features2.foreach { stat2.observe }
//
//          minMax2.min mustEqual 100L
//          minMax2.max mustEqual 199L
//
//          stat += stat2
//
//          minMax.min mustEqual 0L
//          minMax.max mustEqual 199L
//          minMax2.min mustEqual 100L
//          minMax2.max mustEqual 199L
//
//          "clear them" in {
//            minMax.isEmpty must beFalse
//            minMax2.isEmpty must beFalse
//
//            minMax.clear()
//            minMax2.clear()
//
//            minMax.min mustEqual java.lang.Long.MAX_VALUE
//            minMax.max mustEqual java.lang.Long.MIN_VALUE
//            minMax2.min mustEqual java.lang.Long.MAX_VALUE
//            minMax2.max mustEqual java.lang.Long.MIN_VALUE
//          }
//        }
//      }
//
//      "doubles" in {
//        val stat = Stat(sft, "MinMax(doubleAttr)")
//        val minMax = stat.asInstanceOf[MinMax[java.lang.Double]]
//
//        minMax.attribute mustEqual doubleIndex
//        minMax.min mustEqual java.lang.Double.MAX_VALUE
//        minMax.max mustEqual java.lang.Double.MIN_VALUE
//        minMax.isEmpty must beFalse
//
//        features.foreach { stat.observe }
//
//        minMax.min mustEqual 0
//        minMax.max mustEqual 99
//
//        "serialize and deserialize" in {
//          val packed   = StatSerialization.pack(minMax, sft)
//          val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[MinMax[java.lang.Double]]
//
//          unpacked.toJson() mustEqual minMax.toJson()
//        }
//
//        "serialize and deserialize empty MinMax" in {
//          val stat = Stat(sft, "MinMax(doubleAttr)")
//          val minMax = stat.asInstanceOf[MinMax[java.lang.Double]]
//          val packed   = StatSerialization.pack(minMax, sft)
//          val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[MinMax[java.lang.Double]]
//
//          unpacked.toJson() mustEqual minMax.toJson()
//        }
//
//        "combine two MinMaxes" in {
//          val stat2 = Stat(sft, "MinMax(doubleAttr)")
//          val minMax2 = stat2.asInstanceOf[MinMax[java.lang.Double]]
//
//          features2.foreach { stat2.observe }
//
//          minMax2.min mustEqual 100
//          minMax2.max mustEqual 199
//
//          stat += stat2
//
//          minMax.min mustEqual 0
//          minMax.max mustEqual 199
//          minMax2.min mustEqual 100
//          minMax2.max mustEqual 199
//
//          "clear them" in {
//            minMax.isEmpty must beFalse
//            minMax2.isEmpty must beFalse
//
//            minMax.clear()
//            minMax2.clear()
//
//            minMax.min mustEqual java.lang.Double.MAX_VALUE
//            minMax.max mustEqual java.lang.Double.MIN_VALUE
//            minMax2.min mustEqual java.lang.Double.MAX_VALUE
//            minMax2.max mustEqual java.lang.Double.MIN_VALUE
//          }
//        }
//      }
//
//      "floats" in {
//        val stat = Stat(sft, "MinMax(floatAttr)")
//        val minMax = stat.asInstanceOf[MinMax[java.lang.Float]]
//
//        minMax.attribute mustEqual floatIndex
//        minMax.min mustEqual java.lang.Float.MAX_VALUE
//        minMax.max mustEqual java.lang.Float.MIN_VALUE
//        minMax.isEmpty must beFalse
//
//        features.foreach { stat.observe }
//
//        minMax.min mustEqual 0
//        minMax.max mustEqual 99
//
//        "serialize and deserialize" in {
//          val packed   = StatSerialization.pack(minMax, sft)
//          val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[MinMax[java.lang.Float]]
//
//          unpacked.toJson() mustEqual minMax.toJson()
//        }
//
//        "serialize and deserialize empty MinMax" in {
//          val stat = Stat(sft, "MinMax(floatAttr)")
//          val minMax = stat.asInstanceOf[MinMax[java.lang.Float]]
//          val packed   = StatSerialization.pack(minMax, sft)
//          val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[MinMax[java.lang.Float]]
//
//          unpacked.toJson() mustEqual minMax.toJson()
//        }
//
//        "combine two MinMaxes" in {
//          val stat2 = Stat(sft, "MinMax(floatAttr)")
//          val minMax2 = stat2.asInstanceOf[MinMax[java.lang.Float]]
//
//          features2.foreach { stat2.observe }
//
//          minMax2.min mustEqual 100
//          minMax2.max mustEqual 199
//
//          stat += stat2
//
//          minMax.min mustEqual 0
//          minMax.max mustEqual 199
//          minMax2.min mustEqual 100
//          minMax2.max mustEqual 199
//
//          "clear them" in {
//            minMax.isEmpty must beFalse
//            minMax2.isEmpty must beFalse
//
//            minMax.clear()
//            minMax2.clear()
//
//            minMax.min mustEqual java.lang.Float.MAX_VALUE
//            minMax.max mustEqual java.lang.Float.MIN_VALUE
//            minMax2.min mustEqual java.lang.Float.MAX_VALUE
//            minMax2.max mustEqual java.lang.Float.MIN_VALUE
//          }
//        }
//      }
  }
}
