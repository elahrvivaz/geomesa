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

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Z3Test extends Specification {

  val rand = new Random(-574)
  val maxInt = Math.pow(2, Z3.MAX_BITS - 1).toInt
  def nextDim() = rand.nextInt(maxInt)

  def padTo(s: String) = (new String(Array.fill(63)('0')) + s).takeRight(63)

  "Z3" should {

    "apply and unapply" >> {
      val (x, y, t) = (nextDim(), nextDim(), nextDim())
      val z = Z3(x, y, t)
      z match { case Z3(zx, zy, zt) =>
        zx mustEqual x
        zy mustEqual y
        zt mustEqual t
      }
    }

    "apply and unapply min values" >> {
      val (x, y, t) = (0, 0, 0)
      val z = Z3(x, y, t)
      z match {
        case Z3(zx, zy, zt) =>
          zx mustEqual x
          zy mustEqual y
          zt mustEqual t
      }
    }

    "apply and unapply max values" >> {
      val z3curve = new Z3SFC
      val (x, y, t) = (z3curve.xprec, z3curve.yprec, z3curve.tprec)
      val z = Z3(x.toInt, y.toInt, t.toInt)
      z match { case Z3(zx, zy, zt) =>
        zx mustEqual x
        zy mustEqual y
        zt mustEqual t
      }
    }

    "split" >> {
      val splits = Seq(
        0x00000000ffffffL,
        0x00000000000000L,
        0x00000000000001L,
        0x000000000c0f02L,
        0x00000000000802L
      ) ++ (0 until 10).map(_ => nextDim().toLong)
      splits.foreach { l =>
        val expected = padTo(new String(l.toBinaryString.toCharArray.flatMap(c => s"00$c")))
        padTo(Z3.split(l).toBinaryString) mustEqual expected
      }
      success
    }

    "split and combine" >> {
      val z = nextDim()
      val split = Z3.split(z)
      val combined = Z3.combine(split)
      combined.toInt mustEqual z
    }

    "support mid" >> {
      val (x, y, z)    = (0, 0, 0)
      val (x2, y2, z2) = (2, 2, 2)
      Z3(x, y, z).mid(Z3(x2, y2, z2)) match {
        case Z3(midx, midy, midz) =>
          midx mustEqual 1
          midy mustEqual 1
          midz mustEqual 1
      }
    }

    "support bigmin" >> {
      val zmin = Z3(2, 2, 0)
      val zmax = Z3(3, 6, 0)
      val f = Z3(5, 1, 0)
      val (_, bigmin) = Z3.zdivide(f, zmin, zmax)
      bigmin match {
        case Z3(xhi, yhi, zhi) =>
          xhi mustEqual 2
          yhi mustEqual 4
          zhi mustEqual 0
      }
    }

    "support litmax" >> {
      val zmin = Z3(2, 2, 0)
      val zmax = Z3(3, 6, 0)
      val f = Z3(1, 7, 0)
      val (litmax, _) = Z3.zdivide(f, zmin, zmax)
      litmax match {
        case Z3(xlow, ylow, zlow) =>
          xlow mustEqual 3
          ylow mustEqual 5
          zlow mustEqual 0
      }
    }

    "support in range" >> {
      val (x, y, z) = (nextDim(), nextDim(), nextDim())
      val z3 = Z3(x, y , z)
      val lessx  = Z3(x - 1, y, z)
      val lessx2 = Z3(x - 2, y, z)
      val lessy  = Z3(x, y - 1, z)
      val lessy2 = Z3(x, y - 2, z)
      val lessz  = Z3(x, y, z - 1)
      val lessz2 = Z3(x, y, z - 2)
      val less1  = Z3(x - 1, y - 1, z - 1)
      val less2  = Z3(x - 2, y - 2, z - 2)
      val morex  = Z3(x + 1, y, z)
      val morex2 = Z3(x + 2, y, z)
      val morey  = Z3(x, y + 1, z)
      val morez  = Z3(x, y, z + 1)
      val more1  = Z3(x + 1, y + 1, z + 1)

      z3.inRange(lessx, morex) must beTrue
      z3.inRange(lessx, morey) must beTrue
      z3.inRange(lessx, morez) must beTrue
      z3.inRange(lessx, more1) must beTrue

      z3.inRange(lessy, morex) must beTrue
      z3.inRange(lessy, morey) must beTrue
      z3.inRange(lessy, morez) must beTrue
      z3.inRange(lessy, more1) must beTrue

      z3.inRange(lessz, morex) must beTrue
      z3.inRange(lessz, morey) must beTrue
      z3.inRange(lessz, morez) must beTrue
      z3.inRange(lessz, more1) must beTrue

      z3.inRange(less1, more1) must beTrue

      z3.inRange(more1, less1) must beFalse
      z3.inRange(morex, morex2) must beFalse
      z3.inRange(lessx2, lessx) must beFalse
      z3.inRange(lessy2, lessy) must beFalse
      z3.inRange(lessz2, lessx) must beFalse
      z3.inRange(less2, less1) must beFalse
      z3.inRange(less2, more1) must beTrue
    }

    "calculate ranges" >> {
      val min = Z3(2, 2, 0)
      val max = Z3(3, 6, 0)
      val ranges = Z3.zranges(min, max, 100)
      ranges must haveLength(3)
      ranges must containTheSameElementsAs(Seq((Z3(2, 2, 0).z, Z3(3, 3, 0).z),
        (Z3(2, 4, 0).z, Z3(3, 5, 0).z), (Z3(2, 6, 0).z, Z3(3, 6, 0).z)))
    }

    "return non-empty ranges for a number of cases" >> {
      val ranges = Seq(
        (new Z3(0), new Z3(3961898555948381951l)),
        (new Z3(0), new Z3(4611686018427387903l)),
        (new Z3(0), new Z3(8645783181317881599l)),
        (new Z3(3754665181066933258l), new Z3(3810634797035430054l)),
        (new Z3(3754665181066933258l), new Z3(4460422259514436006l)),
        (new Z3(3759195186253968780l), new Z3(3842344833344678918l)),
        (new Z3(3759195186253968780l), new Z3(3873808578109778054l)),
        (new Z3(3759336199158152616l), new Z3(3873808578109778054l)),
        (new Z3(3763173268547295545l), new Z3(3763173268556857195l)),
        (new Z3(3763831019838881066l), new Z3(4460422259514436006l)),
        (new Z3(73185697271398432l), new Z3(4611686018427387903l)),
        (new Z3(9165529533122820l), new Z3(3963006867897054943l)),
        (new Z3(9165838771947808l), new Z3(4611686018427387903l))
      )

      forall(ranges) {
        r =>
          val ret = Z3.zranges(r._1, r._2, 8)
          ret.length must be greaterThan(0)
      }
    } 
  }
}
