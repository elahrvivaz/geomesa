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
class SeqStatTest extends Specification with StatTestHelper {

  def newStat[T](observe: Boolean = true): SeqStat = {
    val stat = Stat(sft, "MinMax(intAttr);IteratorStackCount();Histogram(longAttr);RangeHistogram(doubleAttr,20,0,200)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[SeqStat]
  }

  "Seq stat" should {

    "be empty initiallly" >> {
      val stat = newStat(observe = false)

      stat.stats must haveSize(4)
      stat.isEmpty must beFalse

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[Histogram[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[RangeHistogram[java.lang.Double]]

      mm.attribute mustEqual intIndex
      mm.min must beNull
      mm.max must beNull

      ic.counter mustEqual 1

      eh.attribute mustEqual longIndex
      eh.histogram must beEmpty

      rh.attribute mustEqual doubleIndex
      forall(0 until rh.numBins)(rh.bins(_) mustEqual 0)
    }

    "observe correct values" >> {
      val stat = newStat()

      val stats = stat.stats

      stats must haveSize(4)
      stat.isEmpty must beFalse

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[Histogram[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[RangeHistogram[java.lang.Double]]

      mm.min mustEqual 0
      mm.max mustEqual 99

      ic.counter mustEqual 1

      eh.histogram.size mustEqual 100
      eh.histogram(0L) mustEqual 1
      eh.histogram(100L) mustEqual 0

      rh.bins.length mustEqual 20
      rh.bins(rh.bins.indexOf(0.0)) mustEqual 10
      rh.bins(rh.bins.indexOf(50.0)) mustEqual 10
      rh.bins(rh.bins.indexOf(100.0)) mustEqual 0
    }

    "serialize to json" >> {
      val stat = newStat()
      stat.toJson() must not(beEmpty)
    }

    "serialize empty to json" >> {
      val stat = newStat(observe = false)
      stat.toJson() must not(beEmpty)
    }

    "serialize and deserialize" >> {
      val stat = newStat()
      val packed = StatSerializer(sft).serialize(stat)
      val unpacked = StatSerializer(sft).deserialize(packed)
      unpacked.toJson() mustEqual stat.toJson()
    }

    "serialize and deserialize empty SeqStat" >> {
      val minMax = newStat(observe = false)
      val packed = StatSerializer(sft).serialize(minMax)
      val unpacked = StatSerializer(sft).deserialize(packed)
      unpacked.toJson() mustEqual minMax.toJson()
    }

    "combine two SeqStats" >> {
      val stat = newStat()
      val stat2 = newStat(observe = false)

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[Histogram[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[RangeHistogram[java.lang.Double]]

      val mm2 = stat2.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic2 = stat2.stats(1).asInstanceOf[IteratorStackCount]
      val eh2 = stat2.stats(2).asInstanceOf[Histogram[java.lang.Long]]
      val rh2 = stat2.stats(3).asInstanceOf[RangeHistogram[java.lang.Double]]

      mm2.min must beNull
      mm2.max must beNull

      ic2.counter mustEqual 1

      eh2.histogram must beEmpty

      rh2.bins.length mustEqual 20
      forall(0 until 20)(rh2.bins(_) mustEqual 0)

      features2.foreach { stat2.observe }

      stat += stat2

      mm.min mustEqual 0
      mm.max mustEqual 199

      ic.counter mustEqual 2

      eh.histogram.size mustEqual 200
      eh.histogram(0L) mustEqual 1
      eh.histogram(100L) mustEqual 1

      rh.bins.length mustEqual 20
      rh.bins(rh.bins.indexOf(0.0)) mustEqual 10
      rh.bins(rh.bins.indexOf(50.0)) mustEqual 10
      rh.bins(rh.bins.indexOf(100.0)) mustEqual 10

      mm2.min mustEqual 100
      mm2.max mustEqual 199

      ic2.counter mustEqual 1

      eh2.histogram.size mustEqual 100
      eh2.histogram(0L) mustEqual 0
      eh2.histogram(100L) mustEqual 1

      rh2.bins.length mustEqual 20
      rh2.bins(rh2.bins.indexOf(0.0)) mustEqual 0
      rh2.bins(rh2.bins.indexOf(50.0)) mustEqual 0
      rh2.bins(rh2.bins.indexOf(100.0)) mustEqual 10
    }

    "clear" >> {
      val stat = newStat()
      stat.isEmpty must beFalse

      stat.clear()

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[Histogram[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[RangeHistogram[java.lang.Double]]

      mm.attribute mustEqual intIndex
      mm.min must beNull
      mm.max must beNull

      ic.counter mustEqual 1

      eh.attribute mustEqual longIndex
      eh.histogram must beEmpty

      rh.attribute mustEqual doubleIndex
      forall(0 until rh.numBins)(rh.bins(_) mustEqual 0)
    }
  }
}
