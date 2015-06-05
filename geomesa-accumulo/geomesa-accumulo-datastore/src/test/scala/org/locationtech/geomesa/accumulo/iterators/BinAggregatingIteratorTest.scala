/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayOutputStream, FileInputStream}
import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.INTERNAL_GEOMESA_VERSION
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationType}
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BinAggregatingIteratorTest extends Specification {

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType(getClass.getSimpleName, spec)
  val r = new Random(10)

  val features = (0 until 110).map { i =>
    val dtg = new Date(Math.abs(r.nextInt(999999)))
    val name = s"name$i"
//    val dtg = s"2010-05-07T0${9 - i}:00:00.000Z"
    val geom = s"POINT(40 6$i)"
    val sf = new ScalaSimpleFeature(s"$i", sft)
    sf.setAttributes(Array[AnyRef](name, dtg, geom))
    sf
  }

  "BinAggregatingIterator" should {
//    "sort aggregated records" in {
//      val out = new ByteArrayOutputStream(16 * features.length)
//      features.foreach { f =>
//        val values =
//          BasicValues(f.getDefaultGeometry.asInstanceOf[Point].getY.toFloat,
//            f.getDefaultGeometry.asInstanceOf[Point].getX.toFloat,
//            f.getAttribute("dtg").asInstanceOf[Date].getTime,
//            Some(f.getAttribute("name").asInstanceOf[String]))
//        Convert2ViewerFunction.encode(values, out)
//      }
//      val bytes = out.toByteArray
//      BinAggregatingIterator.sortByChunks(bytes, bytes.length, 16)
//      val recovered = bytes.grouped(16).map(Convert2ViewerFunction.decode).map(_.dtg).toSeq
//      recovered.foreach(println)
//      val expected = recovered.sorted
//      recovered mustEqual expected
//      success
//    }
    "quicksort" in {
      val out = new ByteArrayOutputStream(16 * features.length)
      features.foreach { f =>
        val values =
          BasicValues(f.getDefaultGeometry.asInstanceOf[Point].getY.toFloat,
            f.getDefaultGeometry.asInstanceOf[Point].getX.toFloat,
            f.getAttribute("dtg").asInstanceOf[Date].getTime,
            Some(f.getAttribute("name").asInstanceOf[String]))
        Convert2ViewerFunction.encode(values, out)
      }
      val bytes = out.toByteArray
      println(bytes.length + " total")
      val expected = bytes.grouped(16).map(Convert2ViewerFunction.decode).map(_.dtg).toSeq.sorted
      println(expected.mkString(" "))
      BinSorter.quickSort(bytes, 0, bytes.length - 16)

      val actual = bytes.grouped(16).map(Convert2ViewerFunction.decode).map(_.dtg).toSeq

      println(actual.mkString(" "))

      actual mustEqual expected
    }
  }
}
