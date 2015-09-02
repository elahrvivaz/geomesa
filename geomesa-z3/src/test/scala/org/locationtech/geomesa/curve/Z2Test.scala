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
class Z2Test extends Specification {

  val rand = new Random(-574)
  val maxInt = Z2SFC.xprec.toInt
  def nextDim() = rand.nextInt(maxInt)

  def padTo(s: String) = (new String(Array.fill(63)('0')) + s).takeRight(63)

  "Z2" should {

    "apply and unapply" >> {
      val (x, y) = (nextDim(), nextDim())
      val z = Z2(x, y)
      val (zx, zy) = z.decode
      zx mustEqual x
      zy mustEqual y
    }

    "apply and unapply min values" >> {
      val (x, y) = (0, 0)
      val z = Z2(x, y)
      val (zx, zy) = z.decode
      zx mustEqual x
      zy mustEqual y
    }

    "apply and unapply max values" >> {
      val Z2curve = Z2SFC
      val (x, y) = (Z2curve.xprec, Z2curve.yprec)
      val z = Z2(x.toInt, y.toInt)
      val (zx, zy) = z.decode
      zx mustEqual x
      zy mustEqual y
    }

    "split" >> {
      val splits = Seq(
        0x0000003fffffffL,
        0x00000000000000L,
        0x00000000000001L,
        0x000000000c0f02L,
        0x00000000000802L
      ) ++ (0 until 10).map(_ => nextDim().toLong)
      splits.foreach { l =>
        val expected = padTo(new String(l.toBinaryString.toCharArray.flatMap(c => s"0$c")))
        padTo(Z2.split(l).toBinaryString) mustEqual expected
      }
      success
    }

    "split and combine" >> {
      val z = nextDim()
      val split = Z2.split(z)
      val combined = Z2.combine(split)
      combined.toInt mustEqual z
    }
  }
}
