/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatTest extends Specification with StatTestHelper {

  sequential

  "stats" should {
    "fail for malformed strings" in {
      Stat(sft, "") must throwAn[Exception]
      Stat(sft, "abcd") must throwAn[Exception]
      Stat(sft, "RangeHistogram()") must throwAn[Exception]
      Stat(sft, "RangeHistogram(foo,10,2012-01-01T00:00:00.000Z,2012-02-01T00:00:00.000Z)") must throwAn[Exception]
      Stat(sft, "MinMax()") must throwAn[Exception]
      Stat(sft, "MinMax(abcd)") must throwAn[Exception]
    }

    "create a sequence of stats" in {
      val stat = Stat(sft, "MinMax(intAttr);IteratorStackCounter;EnumeratedHistogram(longAttr);RangeHistogram(doubleAttr,20,0,200)")
      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 4
      stat.isEmpty must beFalse

      val minMax = stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val isc = stats(1).asInstanceOf[IteratorStackCounter]
      val eh = stats(2).asInstanceOf[EnumeratedHistogram[java.lang.Long]]
      val rh = stats(3).asInstanceOf[RangeHistogram[java.lang.Double]]

      minMax.attribute mustEqual intIndex
      minMax.min must beNull
      minMax.max must beNull

      isc.count mustEqual 1

      eh.attribute mustEqual longIndex
      eh.frequencyMap.size mustEqual 0

      rh.attribute mustEqual doubleIndex
      rh.bins.length mustEqual 20
      rh.bins(rh.bins.getIndex(0.0)) mustEqual 0
      rh.bins(rh.bins.getIndex(50.0)) mustEqual 0
      rh.bins(rh.bins.getIndex(100.0)) mustEqual 0

      features.foreach { stat.observe }

      minMax.min mustEqual 0
      minMax.max mustEqual 99

      isc.count mustEqual 1

      eh.frequencyMap.size mustEqual 100
      eh.frequencyMap(0L) mustEqual 1
      eh.frequencyMap(100L) mustEqual 0

      rh.bins.length mustEqual 20
      rh.bins(rh.bins.getIndex(0.0)) mustEqual 10
      rh.bins(rh.bins.getIndex(50.0)) mustEqual 10
      rh.bins(rh.bins.getIndex(100.0)) mustEqual 0

      "serialize and deserialize" in {
        val packed   = StatSerialization.pack(stat, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[SeqStat]

        unpacked.toJson() mustEqual stat.asInstanceOf[SeqStat].toJson()
      }

      "combine two SeqStats" in {
        val stat2 = Stat(sft, "MinMax(intAttr);IteratorStackCounter;EnumeratedHistogram(longAttr);RangeHistogram(doubleAttr,20,0,200)")
        val stats2 = stat2.asInstanceOf[SeqStat].stats

        stats2.size mustEqual 4

        val minMax2 = stats2(0).asInstanceOf[MinMax[java.lang.Integer]]
        val isc2 = stats2(1).asInstanceOf[IteratorStackCounter]
        val eh2 = stats2(2).asInstanceOf[EnumeratedHistogram[java.lang.Long]]
        val rh2 = stats2(3).asInstanceOf[RangeHistogram[java.lang.Double]]

        features2.foreach { stat2.observe }

        stat += stat2

        minMax.min mustEqual 0
        minMax.max mustEqual 199

        isc.count mustEqual 2

        eh.frequencyMap.size mustEqual 200
        eh.frequencyMap(0L) mustEqual 1
        eh.frequencyMap(100L) mustEqual 1

        rh.bins.length mustEqual 20
        rh.bins(rh.bins.getIndex(0.0)) mustEqual 10
        rh.bins(rh.bins.getIndex(50.0)) mustEqual 10
        rh.bins(rh.bins.getIndex(100.0)) mustEqual 10

        minMax2.min mustEqual 100
        minMax2.max mustEqual 199

        isc2.count mustEqual 1

        eh2.frequencyMap.size mustEqual 100
        eh2.frequencyMap(0L) mustEqual 0
        eh2.frequencyMap(100L) mustEqual 1

        rh2.bins.length mustEqual 20
        rh2.bins(rh2.bins.getIndex(0.0)) mustEqual 0
        rh2.bins(rh2.bins.getIndex(50.0)) mustEqual 0
        rh2.bins(rh2.bins.getIndex(100.0)) mustEqual 10

        "clear them" in {
          stat.isEmpty must beFalse
          stat2.isEmpty must beFalse

          stat.clear()
          stat2.clear()

          minMax.min must beNull
          minMax.max must beNull

          isc.count mustEqual 1

          eh.frequencyMap.size mustEqual 0

          rh.bins.length mustEqual 20
          rh.bins(rh.bins.getIndex(0.0)) mustEqual 0
          rh.bins(rh.bins.getIndex(50.0)) mustEqual 0
          rh.bins(rh.bins.getIndex(100.0)) mustEqual 0

          minMax2.min must beNull
          minMax2.max must beNull

          isc2.count mustEqual 1

          eh2.frequencyMap.size mustEqual 0

          rh2.bins.length mustEqual 20
          rh2.bins(rh2.bins.getIndex(0.0)) mustEqual 0
          rh2.bins(rh2.bins.getIndex(50.0)) mustEqual 0
          rh2.bins(rh2.bins.getIndex(100.0)) mustEqual 0
        }
      }
    }
  }
}
