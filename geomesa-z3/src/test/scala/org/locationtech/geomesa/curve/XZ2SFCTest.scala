/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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
class XZ2SFCTest extends Specification {

  val sfc = new XZ2SFC(12)


  type MyTuple = (Long, Long)

  "XZ2" should {
    "index polygons and query them" >> {
      val poly = sfc.index(10, 10, 12, 12)
      println(poly)
      val matches = (poly, poly).zip(beLessThanOrEqualTo[Long], beGreaterThanOrEqualTo[Long])

      val containing = Seq(
        (9.0, 9.0, 13.0, 13.0),
        (-180.0, -90.0, 180.0, 90.0),
        (0.0, 0.0, 180.0, 90.0),
        (0.0, 0.0, 20.0, 20.0)
      )
      val overlapping = Seq(
        (11.0, 11.0, 13.0, 13.0),
        (9.0, 9.0, 11.0, 11.0),
        (10.5, 10.5, 11.5, 11.5),
        (11.0, 11.0, 11.0, 11.0)
      )
      val disjoint = Seq(
        (-90.0, -180.0, 8.0, 8.0),
        (0.0, 0.0, 8.0, 8.0),
        (9.0, 9.0, 9.5, 9.5),
        (12.5, 12.5, 13.5, 13.5),
        (20.0, 20.0, 180.0, 90.0)
      )
      forall(containing ++ overlapping) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        println(bbox + " " + ranges)
        ranges must contain(matches)
      }
      forall(disjoint) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        ranges must not(contain(matches))
      }
    }
  }
}
