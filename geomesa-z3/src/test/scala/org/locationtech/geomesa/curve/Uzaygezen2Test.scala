/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.hilbert.UzaygezenHilbert2SFC
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Uzaygezen2Test extends Specification {

  val rand = new Random(-574)
  val maxInt = UzaygezenHilbert2SFC.dx.maxIndex
  def nextDim(): Int = rand.nextInt(maxInt)

  def padTo(s: String): String = (new String(Array.fill(62)('0')) + s).takeRight(62)

  "Hilbert2" should {

    "fail for out-of-bounds values" >> {
      val sfc = UzaygezenHilbert2SFC
      foreach(Seq((-180.1, 0d), (0d, -90.1), (180.1, 0d), (0d, 90.1), (-181d, -91d), (181d, 91d))) {
        case (x, y) => sfc.index(x, y) must throwAn[IllegalArgumentException]
      }
    }

    "return non-empty ranges for a number of cases" >> {
      val sfc = UzaygezenHilbert2SFC
      val ranges = Seq[(Double, Double, Double, Double)](
        (-180, -90, 180, 90),                // whole world
//        (-90, -45, 90, 45),                  // half world
//        (35, 65, 45, 75),                    // 10^2 degrees
//        (35, 55, 45, 75),                    // 10x20 degrees
        (35, 65, 37, 68),                    // 2x3 degrees
//        (35, 65, 40, 70),                    // 5^2 degrees
        (39.999, 60.999, 40.001, 61.001),    // small bounds
        (51.0, 51.0, 51.1, 51.1),            // small bounds
        (51.0, 51.0, 51.001, 51.001),        // small bounds
        (51.0, 51.0, 51.0000001, 51.0000001) // 60 bits in common
      )

      def print(l: Long, u: Long, size: Int): Unit =
        println(s"${round(sfc.invert(l))} ${round(sfc.invert(u))}\t$size")
      def round(z: (Double, Double)): (Double, Double) =
        (math.round(z._1 * 1000.0) / 1000.0, math.round(z._2 * 1000.0) / 1000.0)

      foreach(ranges) { range =>
        val ret = sfc.ranges(Seq(range))
        println(range  + " " + ret.length)
        ret.length must beGreaterThan(0)
      }
    }
  }
}
