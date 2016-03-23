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
class RangeHistogramTest extends Specification with StatTestHelper {

  sequential

  "RangeHistogram stat" should {

    "work with strings" >> {
      val stat = Stat(sft, "RangeHistogram(strAttr,20,abc000,abc200)")
      val rh = stat.asInstanceOf[RangeHistogram[String]]
      val lowerEndpoint = "abc000"
      val midpoint = "abc100"

      val lowerIndex = rh.bins.getIndex(lowerEndpoint)
      val middleIndex = rh.bins.getIndex(midpoint)

      features.foreach { stat.observe }

      "correctly bin values"  >> {
        rh.isEmpty must beFalse
        rh.bins.length mustEqual 20
        rh.bins(lowerIndex) mustEqual 40
        rh.bins(middleIndex) mustEqual 0
      }

      "serialize and deserialize" >> {
        val packed   = StatSerialization.pack(rh, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[RangeHistogram[String]]

        unpacked.numBins mustEqual rh.numBins
        unpacked.attribute mustEqual rh.attribute
        unpacked.toJson() mustEqual rh.toJson()
      }

      "combine two RangeHistograms" >> {
        val stat2 = Stat(sft, "RangeHistogram(strAttr,20,abc000,abc200)")
        val rh2 = stat2.asInstanceOf[RangeHistogram[String]]

        features2.foreach { stat2.observe }

        rh2.bins.length mustEqual 20
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 40

        stat += stat2

        rh.bins.length mustEqual 20
        rh.bins(lowerIndex) mustEqual 40
        rh.bins(middleIndex) mustEqual 40
        rh2.bins.length mustEqual 20
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 40
      }

      "clear" >> {
        rh.clear()

        rh.isEmpty must beFalse
        rh.bins.length mustEqual 20
        rh.bins(lowerIndex) mustEqual 0
        rh.bins(middleIndex) mustEqual 0
      }
    }

    "work with integers" >> {
      val stat = Stat(sft, "RangeHistogram(intAttr,20,0,200)")
      val rh = stat.asInstanceOf[RangeHistogram[java.lang.Integer]]
      val lowerEndpoint = 0
      val midpoint = 100

      val lowerIndex = rh.bins.getIndex(lowerEndpoint)
      val middleIndex = rh.bins.getIndex(midpoint)

      features.foreach { stat.observe }

      "correctly bin values"  >> {
        rh.isEmpty must beFalse
        rh.bins.length mustEqual 20
        rh.bins(lowerIndex) mustEqual 10
        rh.bins(middleIndex) mustEqual 0
      }

      "serialize and deserialize" >> {
        val packed   = StatSerialization.pack(rh, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[RangeHistogram[java.lang.Integer]]

        unpacked.numBins mustEqual rh.numBins
        unpacked.attribute mustEqual rh.attribute
        unpacked.toJson() mustEqual rh.toJson()
      }

      "combine two RangeHistograms" >> {
        val stat2 = Stat(sft, "RangeHistogram(intAttr,20,0,200)")
        val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Integer]]

        features2.foreach { stat2.observe }

        rh2.bins.length mustEqual 20
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 10

        stat += stat2

        rh.bins.length mustEqual 20
        rh.bins(lowerIndex) mustEqual 10
        rh.bins(middleIndex) mustEqual 10
        rh2.bins.length mustEqual 20
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 10
      }

      "clear" >> {
        rh.clear()

        rh.isEmpty must beFalse
        rh.bins.length mustEqual 20
        rh.bins(lowerIndex) mustEqual 0
        rh.bins(middleIndex) mustEqual 0
      }
    }

    "work with longs" >> {
      val stat = Stat(sft, "RangeHistogram(longAttr,7,90,110)")
      val rh = stat.asInstanceOf[RangeHistogram[java.lang.Long]]
      val lowerEndpoint = 90L
      val midpoint = 96L
      val upperEndpoint = 102L

      val lowerIndex = rh.bins.getIndex(lowerEndpoint)
      val middleIndex = rh.bins.getIndex(midpoint)
      val upperIndex = rh.bins.getIndex(upperEndpoint)

      rh.isEmpty must beFalse

      features.foreach { stat.observe }

      "correctly bin features" >> {
        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 3L
        rh.bins(middleIndex) mustEqual 3L
        rh.bins(upperIndex) mustEqual 0L
      }

      "serialize and deserialize" >> {
        val packed   = StatSerialization.pack(rh, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[RangeHistogram[java.lang.Long]]

        unpacked.numBins mustEqual rh.numBins
        unpacked.attribute mustEqual rh.attribute
        unpacked.toJson() mustEqual rh.toJson()
      }

      "combine two RangeHistograms" >> {
        val stat2 = Stat(sft, "RangeHistogram(longAttr,7,90,110)")
        val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Long]]

        features2.foreach { stat2.observe }

        rh2.bins.length mustEqual 7
        rh2.bins(lowerIndex) mustEqual 0L
        rh2.bins(middleIndex) mustEqual 0L
        rh2.bins(upperIndex) mustEqual 3L

        stat += stat2

        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 3L
        rh.bins(middleIndex) mustEqual 3L
        rh.bins(upperIndex) mustEqual 3L
        rh2.bins.length mustEqual 7
        rh2.bins(lowerIndex) mustEqual 0L
        rh2.bins(middleIndex) mustEqual 0L
        rh2.bins(upperIndex) mustEqual 3L
      }

      "clear" >> {
        rh.clear()

        rh.isEmpty must beFalse
        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 0
        rh.bins(middleIndex) mustEqual 0
      }
    }

    "work with floats" >> {
      val stat = Stat(sft, "RangeHistogram(floatAttr,7,90,110)")
      val rh = stat.asInstanceOf[RangeHistogram[java.lang.Float]]
      val lowerEndpoint = 90.0f
      val midpoint = 98.57143f
      val upperEndpoint = 107.14285f

      val lowerIndex = rh.bins.getIndex(lowerEndpoint)
      val middleIndex = rh.bins.getIndex(midpoint)
      val upperIndex = rh.bins.getIndex(upperEndpoint)

      features.foreach { stat.observe }

      "correctly bin values" >> {
        rh.isEmpty must beFalse
        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 3
        rh.bins(middleIndex) mustEqual 3
        rh.bins(upperIndex) mustEqual 0
      }

      "serialize and deserialize" >> {
        val packed   = StatSerialization.pack(rh, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[RangeHistogram[java.lang.Float]]

        unpacked.numBins mustEqual rh.numBins
        unpacked.attribute mustEqual rh.attribute
        unpacked.toJson() mustEqual rh.toJson()
      }

      "combine two RangeHistograms" >> {
        val stat2 = Stat(sft, "RangeHistogram(floatAttr,7,90,110)")
        val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Float]]

        features2.foreach { stat2.observe }

        rh2.bins.length mustEqual 7
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 0
        rh2.bins(upperIndex) mustEqual 3

        stat += stat2

        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 3
        rh.bins(middleIndex) mustEqual 3
        rh.bins(upperIndex) mustEqual 3
        rh2.bins.length mustEqual 7
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 0
        rh2.bins(upperIndex) mustEqual 3
      }

      "clear" >> {
        rh.clear()

        rh.isEmpty must beFalse
        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 0
        rh.bins(middleIndex) mustEqual 0
      }
    }

    "work with doubles" >> {
      val stat = Stat(sft, "RangeHistogram(doubleAttr,7,90,110)")
      val rh = stat.asInstanceOf[RangeHistogram[java.lang.Double]]
      val lowerEndpoint = 90.0
      val midpoint = 98.57142857142857
      val upperEndpoint = 107.14285714285714

      val lowerIndex = rh.bins.getIndex(lowerEndpoint)
      val middleIndex = rh.bins.getIndex(midpoint)
      val upperIndex = rh.bins.getIndex(upperEndpoint)

      features.foreach { stat.observe }

      "correctly bin values" >> {
        rh.isEmpty must beFalse
        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 3
        rh.bins(middleIndex) mustEqual 3
        rh.bins(upperIndex) mustEqual 0
      }

      "serialize and deserialize" >> {
        val packed   = StatSerialization.pack(rh, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[RangeHistogram[java.lang.Double]]

        unpacked.numBins mustEqual rh.numBins
        unpacked.attribute mustEqual rh.attribute
        unpacked.toJson() mustEqual rh.toJson()
      }

      "combine two RangeHistograms" >> {
        val stat2 = Stat(sft, "RangeHistogram(doubleAttr,7,90,110)")
        val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Double]]

        features2.foreach { stat2.observe }

        rh2.bins.length mustEqual 7
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 0
        rh2.bins(upperIndex) mustEqual 3

        stat += stat2

        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 3
        rh.bins(middleIndex) mustEqual 3
        rh.bins(upperIndex) mustEqual 3
        rh2.bins.length mustEqual 7
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 0
        rh2.bins(upperIndex) mustEqual 3
      }

      "clear" >> {
        rh.clear()

        rh.isEmpty must beFalse
        rh.bins.length mustEqual 7
        rh.bins(lowerIndex) mustEqual 0
        rh.bins(middleIndex) mustEqual 0
      }
    }

    "work with dates" >> {
      val stat = Stat(sft, "RangeHistogram(dtg,24,'2012-01-01T00:00:00.000Z','2012-01-03T00:00:00.000Z')")
      val rh = stat.asInstanceOf[RangeHistogram[Date]]
      val lowerEndpoint = GeoToolsDateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
      val midpoint = GeoToolsDateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate

      val lowerIndex = rh.bins.getIndex(lowerEndpoint)
      val middleIndex = rh.bins.getIndex(midpoint)

      features.foreach { stat.observe }

      "correctly bin values"  >> {
        rh.isEmpty must beFalse
        rh.bins.length mustEqual 24

        lowerIndex mustEqual 0
        middleIndex mustEqual 12

        rh.bins(lowerIndex) mustEqual 10
        rh.bins(middleIndex) mustEqual 0
      }

      "serialize and deserialize" >> {
        val packed   = StatSerialization.pack(rh, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[RangeHistogram[Date]]

        unpacked.numBins mustEqual rh.numBins
        unpacked.attribute mustEqual rh.attribute
        unpacked.toJson() mustEqual rh.toJson()
      }

      "combine two RangeHistograms" >> {
        val stat2 = Stat(sft, "RangeHistogram(dtg,24,'2012-01-01T00:00:00.000Z','2012-01-03T00:00:00.000Z')")
        val rh2 = stat2.asInstanceOf[RangeHistogram[Date]]

        features2.foreach { stat2.observe }

        rh2.bins.length mustEqual 24
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 8

        stat += stat2

        rh.bins.length mustEqual 24
        rh.bins(lowerIndex) mustEqual 10
        rh.bins(middleIndex) mustEqual 8
        rh2.bins.length mustEqual 24
        rh2.bins(lowerIndex) mustEqual 0
        rh2.bins(middleIndex) mustEqual 8
      }

      "clear them" >> {
        rh.clear()

        rh.isEmpty must beFalse
        rh.bins.length mustEqual 24
        rh.bins(lowerIndex) mustEqual 0
        rh.bins(middleIndex) mustEqual 0
      }
    }

    "work with geometries" >> {
      val stat = Stat(sft, "RangeHistogram(geom,36,'POINT(-180 -90)','POINT(180 90)')")
      // 4096, 256, 36
      val rh = stat.asInstanceOf[RangeHistogram[Geometry]]
      val lowerEndpoint = WKTUtils.read("POINT(-180 -90)")
      val midpoint = WKTUtils.read("POINT(0 0)")
      val upperEndpoint = WKTUtils.read("POINT(180 90)")

      features.foreach { stat.observe }

      "correctly bin values" >> {
        rh.isEmpty must beFalse
        rh.bins.length mustEqual 36
        rh.bins(12) mustEqual 9
        rh.bins(13) mustEqual 44
        rh.bins(14) mustEqual 45
        rh.bins(15) mustEqual 1
        rh.bins(28) mustEqual 1
        forall((0 until 36).filterNot(i => (i > 11 && i < 16) || i == 28))(rh.bins(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val packed   = StatSerialization.pack(rh, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[RangeHistogram[Geometry]]

        unpacked.numBins mustEqual rh.numBins
        unpacked.attribute mustEqual rh.attribute
        unpacked.toJson() mustEqual rh.toJson()
      }

      "combine two RangeHistograms" >> {
        val stat2 = Stat(sft, "RangeHistogram(geom,36,'POINT(-180 -90)','POINT(180 90)')")
        val rh2 = stat2.asInstanceOf[RangeHistogram[Geometry]]

        features2.foreach { stat2.observe }

        rh2.bins.length mustEqual 36
        rh2.bins(29) mustEqual 10
        rh2.bins(32) mustEqual 20
        rh2.bins(34) mustEqual 25
        rh2.bins(35) mustEqual 45
        forall((0 until 36).filterNot(i => i == 29 || i == 32 || i == 34 || i == 35))(rh2.bins(_) mustEqual 0)

        stat += stat2

        rh.bins.length mustEqual 36
        rh.bins(12) mustEqual 9
        rh.bins(13) mustEqual 44
        rh.bins(14) mustEqual 45
        rh.bins(15) mustEqual 1
        rh.bins(28) mustEqual 1
        rh.bins(29) mustEqual 10
        rh.bins(32) mustEqual 20
        rh.bins(34) mustEqual 25
        rh.bins(35) mustEqual 45
        forall((0 until 36).filterNot(i => (i > 11 && i < 16) || i == 28 || i == 29 || i == 32 || i == 34 || i == 35))(rh.bins(_) mustEqual 0)

        rh2.bins.length mustEqual 36
        rh2.bins(29) mustEqual 10
        rh2.bins(32) mustEqual 20
        rh2.bins(34) mustEqual 25
        rh2.bins(35) mustEqual 45
        forall((0 until 36).filterNot(i => i == 29 || i == 32 || i == 34 || i == 35))(rh2.bins(_) mustEqual 0)
      }

      "clear" >> {
        rh.clear()

        rh.isEmpty must beFalse
        rh.bins.length mustEqual 36
        rh.bins(0) mustEqual 0
        rh.bins(14) mustEqual 0
      }
    }
  }
}
