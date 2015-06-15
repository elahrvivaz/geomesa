/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.index

import java.util.ConcurrentModificationException

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Envelope, Point}
import com.vividsolutions.jts.index.quadtree.Quadtree
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.{Random, Try}

@RunWith(classOf[JUnitRunner])
class BucketIndexTest extends Specification with Logging {

  "BucketIndex" should {
//    "be thread safe" in {
//      val qt = new Quadtree
//      (0 until 20).foreach { i =>
//        val pt = WKTUtils.read(s"POINT(45.$i 50)")
//        val env = pt.getEnvelopeInternal
//        qt.insert(env, i)
//      }
//      println(qt.depth())
//      success
//    }

    "support insert and query" in {
      val index = new BucketIndex[String]()
      val pts = for (x <- -180 to 180; y <- -90 to 90) yield {
        s"POINT($x $y)"
      }
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        index.insert(env, pt)
      }
      for (i <- index.buckets.indices; j <- index.buckets(i).indices) {
        println(i + " " + j)
        index.buckets(i)(j) must haveSize(1)
      }
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        val results = index.query(env).toSeq
        results must haveSize(1)
        results.head mustEqual pt
      }
      success
    }
  }
}


