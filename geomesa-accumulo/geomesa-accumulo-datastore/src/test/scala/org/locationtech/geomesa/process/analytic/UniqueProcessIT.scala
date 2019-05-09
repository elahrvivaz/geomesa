/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import org.geotools.filter.text.cql2.CQL
import org.geotools.util.NullProgressListener
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class UniqueProcessIT extends TestWithFeatureType {

  sequential

  override val spec = "name:String:index=join,weight:Double:index=join,ml:List[String],dtg:Date,*geom:Point:srid=4326"

  lazy val fs = ds.getFeatureSource(sftName)

  step {
    addFeatures({
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      Seq(
        Seq("alice",    20,   Seq.empty.asJava,         "2012-01-01T12:00:00.000Z", geom),
        Seq("alice",    25,   null,                     "2012-01-01T12:00:00.000Z", geom),
        Seq("bill",     21,   Seq("foo", "bar").asJava, "2013-01-01T12:00:00.000Z", geom),
        Seq("bill",     22,   Seq("foo").asJava,        "2013-01-01T12:00:00.000Z", geom),
        Seq("bill",     23,   Seq("foo").asJava,        "2013-01-01T12:00:00.000Z", geom),
        Seq("bob",      30,   Seq("foo").asJava,        "2014-01-01T12:00:00.000Z", geom),
        Seq("charles",  40,   Seq("foo").asJava,        "2014-01-01T12:30:00.000Z", geom),
        Seq("charles",  null, Seq("foo").asJava,        "2014-01-01T12:30:00.000Z", geom)
      ).map(attributes => ScalaSimpleFeature.create(sft, attributes.take(2).mkString, attributes: _*))
    })
  }

  val pl = new NullProgressListener()

  "UniqueProcess" should {

    "return things without a filter" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("alice", "bill", "bob", "charles"))
    }

    "respect a parent filter" in {
      val features = fs.getFeatures(CQL.toFilter("name LIKE 'b%'"))

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("bill", "bob"))
    }

    "be able to use its own filter" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", CQL.toFilter("name LIKE 'b%'"), null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("bill", "bob"))
    }

    "combine parent and own filter" in {
      val features = fs.getFeatures(CQL.toFilter("name LIKE 'b%'"))

      val process = new UniqueProcess
      val results = process.execute(features, "name", CQL.toFilter("weight > 25"), null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("bob"))
    }

    "default to no histogram" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, null, null, null, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names must contain(exactly[Any]("alice", "bill", "bob", "charles"))

      val counts = uniques.flatMap(f => Option(f.getAttribute("count")))
      counts must beEmpty
    }

    "include histogram if requested" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, true, null, null, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names should contain(exactly[Any]("alice", "bill", "bob", "charles"))

      val counts = uniques.map(_.getAttribute("count"))
      counts should contain(exactly[Any](1L, 2L, 2L, 3L))

      val alice = uniques.find(_.getAttribute("value") == "alice").map(_.getAttribute("count"))
      alice must beSome(2)

      val bill = uniques.find(_.getAttribute("value") == "bill").map(_.getAttribute("count"))
      bill must beSome(3)

      val charles = uniques.find(_.getAttribute("value") == "charles").map(_.getAttribute("count"))
      charles must beSome(2)
    }

    "sort by value" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, true, "DESC", null, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names must haveLength(4)
      names(0) mustEqual "charles"
      names(1) mustEqual "bob"
      names(2) mustEqual "bill"
      names(3) mustEqual "alice"

      val counts = uniques.map(_.getAttribute("count"))
      counts should contain(exactly[Any](1L, 2L, 2L, 3L))

      val alice = uniques.find(_.getAttribute("value") == "alice").map(_.getAttribute("count"))
      alice must beSome(2)

      val bill = uniques.find(_.getAttribute("value") == "bill").map(_.getAttribute("count"))
      bill must beSome(3)

      val charles = uniques.find(_.getAttribute("value") == "charles").map(_.getAttribute("count"))
      charles must beSome(2)
    }

    "sort by histogram" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, true, "DESC", true, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names must haveLength(4)
      names(0) mustEqual "bill"
      names(1) mustEqual "alice"
      names(2) mustEqual "charles"
      names(3) mustEqual "bob"

      val counts = uniques.map(_.getAttribute("count"))
      counts should contain(exactly[Any](1L, 2L, 2L, 3L))

      val alice = uniques.find(_.getAttribute("value") == "alice").map(_.getAttribute("count"))
      alice must beSome(2)

      val bill = uniques.find(_.getAttribute("value") == "bill").map(_.getAttribute("count"))
      bill must beSome(3)

      val charles = uniques.find(_.getAttribute("value") == "charles").map(_.getAttribute("count"))
      charles must beSome(2)
    }

    "deal with multi-valued properties correctly" >> {
      val features = fs.getFeatures()
      val proc = new UniqueProcess
      val results = proc.execute(features, "ml", null, true, "DESC", false, pl)
      val uniques = SelfClosingIterator(results.features()).toList
      val values = uniques.map(_.getAttribute("value"))
      "contain 'foo' and 'bar'" >> { values must containTheSameElementsAs(Seq("foo", "bar")) }
      "'foo' must have count 6" >> { uniques.find(_.getAttribute("value") == "foo").map(_.getAttribute("count")) must beSome(6) }
      "'bar' must have count 1" >> { uniques.find(_.getAttribute("value") == "bar").map(_.getAttribute("count")) must beSome(1) }
    }
  }
}
