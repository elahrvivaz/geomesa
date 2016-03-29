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
class IteratorStackCounterTest extends Specification with StatTestHelper {
  sequential

  "IteratorStackCounter stat" should {
    "create an IteratorStackCounter" in {
      val stat = Stat(sft, "IteratorStackCounter()")
      val isc = stat.asInstanceOf[IteratorStackCounter]

      isc.cnt mustEqual 1L
      isc.isEmpty must beFalse

      "serialize and deserialize" in {
        val packed   = StatSerialization.pack(isc, sft)
        val unpacked = StatSerialization.unpack(packed, sft).asInstanceOf[IteratorStackCounter]

        unpacked.toJson() mustEqual isc.toJson()
      }

      "combine two IteratorStackCounters" in {
        val stat2 = Stat(sft, "IteratorStackCounter()")
        val isc2 = stat2.asInstanceOf[IteratorStackCounter]
        isc2.cnt = 5L

        isc += isc2

        isc.cnt mustEqual 6L
        isc2.cnt mustEqual 5L

        "clear them" in {
          isc.isEmpty must beFalse
          isc2.isEmpty must beFalse

          isc.clear()
          isc2.clear()

          isc.cnt mustEqual 1L
          isc2.cnt mustEqual 1L
        }
      }
    }
  }
}
