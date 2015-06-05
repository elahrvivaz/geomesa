/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.io.ByteArrayOutputStream
import java.util.{Arrays, Date}

import com.vividsolutions.jts.geom.Point
import org.apache.accumulo.core.data.{Range => AccRange}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BinAggregatingIteratorTest extends Specification {

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType(getClass.getSimpleName, spec)
  val r = new Random(10)

  val features = (0 until 110).map { i =>
    val dtg = new Date(Math.abs(r.nextInt(999999)))
    val name = s"name$i"
    val geom = s"POINT(40 6$i)"
    val sf = new ScalaSimpleFeature(s"$i", sft)
    sf.setAttributes(Array[AnyRef](name, dtg, geom))
    sf
  }

  val out = new ByteArrayOutputStream(16 * features.length)
  features.foreach { f =>
    val values =
      BasicValues(f.getDefaultGeometry.asInstanceOf[Point].getY.toFloat,
        f.getDefaultGeometry.asInstanceOf[Point].getX.toFloat,
        f.getAttribute("dtg").asInstanceOf[Date].getTime,
        Some(f.getAttribute("name").asInstanceOf[String]))
    Convert2ViewerFunction.encode(values, out)
  }
  val bin = out.toByteArray

  "BinAggregatingIterator" should {
    "quicksort" in {
      val bytes = Arrays.copyOf(bin, bin.length)
      BinSorter.quickSort(bytes, 0, bytes.length - 16)
      val result = bytes.grouped(16).map(Convert2ViewerFunction.decode).map(_.dtg).toSeq
      forall(result.sliding(2))(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
    }
    "mergesort" in {
      val bytes = Arrays.copyOf(bin, bin.length).grouped(48).toSeq
      bytes.foreach(b => BinSorter.quickSort(b, 0, b.length - 16))
      val result = BinSorter.mergeSort(bytes.iterator).map {
        case (b, o) => Convert2ViewerFunction.decode(b.slice(o, o + 16)).dtg
      }
      forall(result.sliding(2))(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
    }
    "mergesort in place" in {
      val bytes = Arrays.copyOf(bin, bin.length).grouped(48)
      val (left, right) = (bytes.next(), bytes.next())
      // sort the left and right arrays
      BinSorter.quickSort(left, 0, left.length - 16)
      BinSorter.quickSort(right, 0, right.length - 16)
      val merged = BinSorter.mergeSort(left, right)
      val result = merged.grouped(16).map(Convert2ViewerFunction.decode).map(_.dtg).toSeq
      forall(result.sliding(2))(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
    }
  }
}
