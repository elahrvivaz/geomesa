/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZRangeTest extends Specification {

  "ZRange" should {

    val zmin = Z3(2, 2, 0)
    val zmax = Z3(3, 6, 0)
    val range = ZRange(zmin, zmax)

    "support overlaps" >> {
      range.overlaps(range) must beTrue
      range.overlaps(ZRange(Z3(3, 0, 0), Z3(3, 2, 0))) must beTrue
      range.overlaps(ZRange(Z3(0, 0, 0), Z3(2, 2, 0))) must beTrue
      range.overlaps(ZRange(Z3(1, 6, 0), Z3(4, 6, 0))) must beTrue
      range.overlaps(ZRange(Z3(2, 0, 0), Z3(3, 1, 0))) must beFalse
      range.overlaps(ZRange(Z3(4, 6, 0), Z3(6, 7, 0))) must beFalse
    }

    "support contains ranges" >> {
      range.contains(range) must beTrue
      range.contains(ZRange(Z3(2, 2, 0), Z3(3, 3, 0))) must beTrue
      range.contains(ZRange(Z3(3, 5, 0), Z3(3, 6, 0))) must beTrue
      range.contains(ZRange(Z3(2, 2, 0), Z3(4, 3, 0))) must beFalse
      range.contains(ZRange(Z3(2, 1, 0), Z3(3, 3, 0))) must beFalse
      range.contains(ZRange(Z3(2, 1, 0), Z3(3, 7, 0))) must beFalse
    }

    "calculate ranges" >> {
      val min = Z3(2, 2, 0)
      val max = Z3(3, 6, 0)
      val ranges = ZRange.zranges(min, max, Z3)
      ranges must haveLength(3)
      ranges must containTheSameElementsAs(Seq((Z3(2, 2, 0).z, Z3(3, 3, 0).z),
        (Z3(2, 4, 0).z, Z3(3, 5, 0).z), (Z3(2, 6, 0).z, Z3(3, 6, 0).z)))
    }

    "return non-empty ranges for a number of cases" >> {
      val sfc = Z3SFC
      val week = sfc.tmax.toLong
      val day = sfc.tmax.toLong / 7
      val hour = sfc.tmax.toLong / 168

      val ranges = Seq(
        (sfc.index(-180, -90, 0), sfc.index(180, 90, week)), // whole world, full week
        (sfc.index(-180, -90, day), sfc.index(180, 90, day * 2)), // whole world, 1 day
        (sfc.index(-180, -90, hour * 10), sfc.index(180, 90, hour * 11)), // whole world, 1 hour
        (sfc.index(-180, -90, hour * 10), sfc.index(180, 90, hour * 64)), // whole world, 54 hours
        (sfc.index(-180, -90, day * 2), sfc.index(180, 90, week)), // whole world, 5 day
        (sfc.index(-90, -45, sfc.tmax.toLong / 4), sfc.index(90, 45, 3 * sfc.tmax.toLong / 4)), // half world, half week
        (sfc.index(35, 65, 0), sfc.index(45, 75, day)), // 10^2 degrees, 1 day
        (sfc.index(35, 55, 0), sfc.index(45, 65, week)), // 10^2 degrees, full week
        (sfc.index(35, 55, day), sfc.index(45, 75, day * 2)), // 10x20 degrees, 1 day
        (sfc.index(35, 55, day + hour * 6), sfc.index(45, 75, day * 2)), // 10x20 degrees, 18 hours
        (sfc.index(35, 65, day + hour), sfc.index(45, 75, day * 6)), // 10^2 degrees, 5 days 23 hours
        (sfc.index(35, 65, day), sfc.index(37, 68, day + hour * 6)), // 2x3 degrees, 6 hours
        (sfc.index(35, 65, day), sfc.index(40, 70, day + hour * 6)), // 5^2 degrees, 6 hours
        (sfc.index(39.999, 60.999, day + 3000), sfc.index(40.001, 61.001, day + 3120)), // small bounds
        (sfc.index(51.0, 51.0, 6000), sfc.index(51.1, 51.1, 6100)), // small bounds
        (sfc.index(51.0, 51.0, 30000), sfc.index(51.001, 51.001, 30100)), // small bounds
        (Z3(sfc.index(51.0, 51.0, 30000).z - 1), Z3(sfc.index(51.0, 51.0, 30000).z + 1)) // 62 bits in common
      )

      def print(l: Z3, u: Z3, size: Int): Unit =
        println(s"${round(sfc.invert(l))} ${round(sfc.invert(u))}\t$size")
      def round(z: (Double, Double, Long)): (Double, Double, Long) =
        (math.round(z._1 * 1000.0) / 1000.0, math.round(z._2 * 1000.0) / 1000.0, z._3)

      forall(ranges) { r =>
        val ret = ZRange.zranges(r._1, r._2, Z3)
        ret.length must beGreaterThan(0)
      }
    }
  }
}
