/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CountStatTest extends Specification with StatTestHelper {

  def newStat(cql: String, observe: Boolean = true): CountStat = {
    val stat = Stat(sft, s"Count('$cql')")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[CountStat]
  }

  "CountStat" should {

    "be empty initiallly" >> {
      val stat = newStat("INCLUDE", observe = false)
      stat.ecql mustEqual "INCLUDE"
      stat.count mustEqual 0L
      stat.isEmpty must beTrue
    }

    "observe correct values" >> {
      val stat = newStat("INCLUDE")
      stat.count mustEqual 100L
    }

    "observe correct values with filters" >> {
      val stat = newStat("intAttr > 49")
      stat.count mustEqual 50L
    }

    "serialize to json" >> {
      val stat = newStat("INCLUDE")
      stat.toJson() mustEqual """{ "count": 100 }"""
    }

    "serialize empty to json" >> {
      val stat = newStat("INCLUDE", observe = false)
      stat.toJson() mustEqual """{ "count": 0 }"""
    }

    "serialize and deserialize" >> {
      val stat = newStat("INCLUDE")
      val packed = StatSerialization.pack(stat, sft)
      val unpacked = StatSerialization.unpack(packed, sft)
      unpacked.toJson() mustEqual stat.toJson()
    }

    "serialize and deserialize empty stat" >> {
      val stat = newStat("INCLUDE", observe = false)
      val packed = StatSerialization.pack(stat, sft)
      val unpacked = StatSerialization.unpack(packed, sft)
      unpacked.toJson() mustEqual stat.toJson()
    }

    "combine two states" >> {
      val stat = newStat("INCLUDE")
      val stat2 = newStat("INCLUDE", observe = false)

      features2.foreach { stat2.observe }

      stat2.count mustEqual 100L

      stat += stat2

      stat.count mustEqual 200L
      stat2.count mustEqual 100L
    }

    "clear" >> {
      val stat = newStat("INCLUDE")
      stat.isEmpty must beFalse

      stat.clear()

      stat.count mustEqual 0L
      stat.isEmpty must beTrue
    }
  }
}
