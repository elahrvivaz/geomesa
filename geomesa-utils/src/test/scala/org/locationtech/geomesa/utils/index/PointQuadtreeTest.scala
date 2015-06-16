/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.index

import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.slf4j.Logging
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class PointQuadtreeTest extends Specification with Logging {

  "PointQuadtree" should {
    "be thread safe" in {
      val numFeatures = 100
      val envelopes = (0 until numFeatures).map(i => (i, WKTUtils.read(s"POINT(45.$i 50)").getEnvelopeInternal)).toArray
      val index = new PointQuadtree[Int]()
      val running = new AtomicBoolean(true)
      val insert = new Thread(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.insert(envelopes(i)._2, i)
          }
        }
      })
      val query = new Thread(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.query(envelopes(i)._2).mkString("")
          }
        }
      })
      val remove = new Thread(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.remove(envelopes(i)._2, i)
          }
        }
      })
      insert.start()
      query.start()
      remove.start()
      Thread.sleep(1000)
      running.set(false)
      insert.join()
      query.join()
      remove.join()
      success
    }.pendingUntilFixed("not implemented")

    "support insert and query" in {
      val index = new PointQuadtree[String]()
      val pts = for (x <- -180 to 180; y <- -90 to 90) yield {
        s"POINT($x $y)"
      }
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        index.insert(env, pt)
      }
//      println(index.print())
      println("searching")
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        val results = index.query(env).toSeq
        results must contain(pt)
      }
      println("searched")
      success
    }.pendingUntilFixed("point quadtree not finished")
  }
}


