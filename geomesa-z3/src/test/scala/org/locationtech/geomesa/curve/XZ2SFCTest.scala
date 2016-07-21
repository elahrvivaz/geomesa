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

  val sfc = new XZ2SFC(6)

  "XZ2" should {
    "index polygons and query them" >> {
      val poly = sfc.index(10, 10, 12, 12)

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
        (-180.0, -90.0, 8.0, 8.0),
        (0.0, 0.0, 8.0, 8.0),
//        (9.0, 9.0, 9.5, 9.5),
        (12.5, 12.5, 13.5, 13.5)
//        (20.0, 20.0, 180.0, 90.0)
      )
      forall(containing ++ overlapping) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (!matches) {
          println(s"$bbox - no match")
        }
        matches must beTrue
      }
      forall(disjoint) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (matches) {
          println(s"$bbox - invalid match")
        }
        matches must beFalse
      }
    }

    "index points and query them" >> {
      val poly = sfc.index(11, 11, 11, 11)

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
      // note: not all disjoint ranges will
      val disjoint = Seq(
        (-180.0, -90.0, 8.0, 8.0),
        (0.0, 0.0, 8.0, 8.0),
//        (9.0, 9.0, 9.5, 9.5),
        (12.5, 12.5, 13.5, 13.5)
//        (20.0, 20.0, 180.0, 90.0)
      )
      forall(containing ++ overlapping) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (!matches) {
          println(s"$bbox - no match")
        }
        matches must beTrue
      }
      forall(disjoint) { bbox =>
        val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
        val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
        if (matches) {
          println(s"$bbox - invalid match")
        }
        matches must beFalse
      }
    }

    "index complex features and query them2" >> {
      val polys = Seq(
        (43.85189184740719,22.40536415671486,47.30967787376657,29.84841540199809),
        (43.85189184740719,22.40536415671486,47.30967787376657,29.84841540199809),
        (41.432203853005966,21.339398405868923,49.7857434014784,27.22357119188849),
        (41.432203853005966,21.339398405868923,49.7857434014784,27.22357119188849),
        (40.71498314879894,21.789879845288173,48.59356551354648,29.715469888517127),
        (40.71498314879894,21.789879845288173,48.59356551354648,29.715469888517127),
        (47.713129661706795,20.09673497300974,48.54487167042291,29.45333238959629),
        (47.713129661706795,20.09673497300974,48.54487167042291,29.45333238959629),
        (40.716236234178005,20.29107512721636,49.719501209537455,29.891171507514056),
        (40.716236234178005,20.29107512721636,49.719501209537455,29.891171507514056),
        (41.67389088584407,20.03913530617221,48.92816957156185,23.890431835536287)
      )

      val ranges = sfc.ranges(Seq((45.0, 23.0, 48.0, 27.0)))
      forall(polys) { p =>
        val index = sfc.index(p._1, p._2, p._3, p._4)
        val matches = ranges.exists(r => r.lower <= index && r.upper >= index)
        if (!matches) {
          println(s"$p - invalid match")
        }
        matches must beTrue
      }
    }

//    "index complex features and query them" >> {
//      val rPoint = """(\d+\.?\d*) (\d+\.?\d*)""".r
//      val file = Source.fromURL(getClass.getClassLoader.getResource("geoms.list"))
//      val geoms = file.getLines().toList.flatMap {
//        case line if line.startsWith("POINT") =>
//         val points = rPoint.findFirstMatchIn(line).map(m => (m.group(1).toDouble, m.group(2).toDouble))
//          points.map(p => (p._1, p._2, p._1, p._2))
//        case line if line.startsWith("LINESTRING") =>
//          val points = rPoint.findAllMatchIn(line).map(m => (m.group(1).toDouble, m.group(2).toDouble)).toSeq
//          Some((points.map(_._1).min, points.map(_._2).min, points.map(_._1).max, points.map(_._2).max))
//        case line if line.startsWith("POLYGON") =>
//          val points = rPoint.findAllMatchIn(line).map(m => (m.group(1).toDouble, m.group(2).toDouble)).toSeq
//          Some((points.map(_._1).min, points.map(_._2).min, points.map(_._1).max, points.map(_._2).max))
//      }
//      file.close()
//
//      val indexedGeoms = geoms.map { case (xmin, ymin, xmax, ymax) => sfc.index(xmin, ymin, xmax, ymax) }
//      val ranges = sfc.ranges(Seq((45.0, 23.0, 48.0, 27.0)))
////      geoms.filter { case (xmin, ymin, xmax, ymax) => val g = sfc.index(xmin, ymin, xmax, ymax); !ranges.exists(r => r.lower <= g && r.upper >= g) }.foreach(println)
//      forall(indexedGeoms) { g =>
//        ranges.exists(r => r.lower <= g && r.upper >= g) must beTrue
//      }
//    }
  }
}