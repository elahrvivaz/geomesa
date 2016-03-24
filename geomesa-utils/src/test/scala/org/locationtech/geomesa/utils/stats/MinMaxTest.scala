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
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
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

    "work with strings" >> {
      "be empty initiallly" >> {
        val minMax = newStat[String]("strAttr", observe = false)
        minMax.attribute mustEqual stringIndex
        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }

      "observe correct values" >> {
        val minMax = newStat[String]("strAttr")
        minMax.min mustEqual "abc000"
        minMax.max mustEqual "abc099"
      }

      "serialize to json" >> {
        val minMax = newStat[String]("strAttr")
        minMax.toJson() mustEqual """{ "min": "abc000", "max": "abc099" }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[String]("strAttr", observe = false)
        minMax.toJson() mustEqual """{ "min": null, "max": null }"""
      }

      "serialize and deserialize" >> {
        val minMax = newStat[String]("strAttr")
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[String]("strAttr", observe = false)
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[String]("strAttr")
        val minMax2 = newStat[String]("strAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.min mustEqual "abc100"
        minMax2.max mustEqual "abc199"

        minMax += minMax2

        minMax.min mustEqual "abc000"
        minMax.max mustEqual "abc199"
        minMax2.min mustEqual "abc100"
        minMax2.max mustEqual "abc199"
      }

      "clear" >> {
        val minMax = newStat[String]("strAttr")
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

    "work with longs" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        minMax.attribute mustEqual longIndex
        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.min mustEqual 0L
        minMax.max mustEqual 99L
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.toJson() mustEqual """{ "min": 0, "max": 99 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        minMax.toJson() mustEqual """{ "min": null, "max": null }"""
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        val minMax2 = newStat[java.lang.Long]("longAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.min mustEqual 100L
        minMax2.max mustEqual 199L

        minMax += minMax2

        minMax.min mustEqual 0L
        minMax.max mustEqual 199L
        minMax2.min mustEqual 100L
        minMax2.max mustEqual 199L
      }

      "clear" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }
    }

    "work with floats" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        minMax.attribute mustEqual floatIndex
        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.min mustEqual 0f
        minMax.max mustEqual 99f
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.toJson() mustEqual """{ "min": 0.0, "max": 99.0 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        minMax.toJson() mustEqual """{ "min": null, "max": null }"""
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        val minMax2 = newStat[java.lang.Float]("floatAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.min mustEqual 100f
        minMax2.max mustEqual 199f

        minMax += minMax2

        minMax.min mustEqual 0f
        minMax.max mustEqual 199f
        minMax2.min mustEqual 100f
        minMax2.max mustEqual 199f
      }

      "clear" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }
    }

    "work with doubles" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        minMax.attribute mustEqual doubleIndex
        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.min mustEqual 0d
        minMax.max mustEqual 99d
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.toJson() mustEqual """{ "min": 0.0, "max": 99.0 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        minMax.toJson() mustEqual """{ "min": null, "max": null }"""
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        val minMax2 = newStat[java.lang.Double]("doubleAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.min mustEqual 100d
        minMax2.max mustEqual 199d

        minMax += minMax2

        minMax.min mustEqual 0d
        minMax.max mustEqual 199d
        minMax2.min mustEqual 100d
        minMax2.max mustEqual 199d
      }

      "clear" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }
    }

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
        minMax.toJson() mustEqual """{ "min": "2012-01-01T00:00:00.000Z", "max": "2012-01-01T23:00:00.000Z" }"""
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

    "work with geometries" >> {
      "be empty initiallly" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        minMax.attribute mustEqual geomIndex
        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }

      "observe correct values" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.min mustEqual WKTUtils.read("POINT (-99 0)")
        minMax.max mustEqual WKTUtils.read("POINT (0 49)")
      }

      "serialize to json" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.toJson() mustEqual """{ "min": "POINT (-99 0)", "max": "POINT (-0 49)" }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        minMax.toJson() mustEqual """{ "min": null, "max": null }"""
      }

      "serialize and deserialize" >> {
        val minMax = newStat[Geometry]("geom")
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        val packed = StatSerialization.pack(minMax, sft)
        val unpacked = StatSerialization.unpack(packed, sft)
        unpacked.toJson() mustEqual minMax.toJson()
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.min mustEqual WKTUtils.read("POINT (80 30)")
        minMax2.max mustEqual WKTUtils.read("POINT (179 79)")

        minMax += minMax2

        minMax.min mustEqual WKTUtils.read("POINT (-99 0)")
        minMax.max mustEqual WKTUtils.read("POINT (179 79)")
        minMax2.min mustEqual WKTUtils.read("POINT (80 30)")
        minMax2.max mustEqual WKTUtils.read("POINT (179 79)")
      }

      "clear" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.min must beNull
        minMax.max must beNull
        minMax.isEmpty must beFalse
      }
    }
  }
}
