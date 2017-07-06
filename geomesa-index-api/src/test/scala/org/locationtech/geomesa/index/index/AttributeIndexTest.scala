/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.index.TestGeoMesaDataStore.{TestQueryPlan, TestRange}
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AttributeIndexTest extends Specification {

  val typeName = "attr-idx-test"
  val spec = "name:String:index=true,age:Int:index=true,height:Float:index=true,dtg:Date,*geom:Point:srid=4326"

  val sft = SimpleFeatureTypes.createType(typeName, spec)

  val df = ISODateTimeFormat.dateTime()

  val aliceGeom   = WKTUtils.read("POINT(45.0 49.0)")
  val billGeom    = WKTUtils.read("POINT(46.0 49.0)")
  val bobGeom     = WKTUtils.read("POINT(47.0 49.0)")
  val charlesGeom = WKTUtils.read("POINT(48.0 49.0)")

  val aliceDate   = df.parseDateTime("2012-01-01T12:00:00.000Z").toDate
  val billDate    = df.parseDateTime("2013-01-01T12:00:00.000Z").toDate
  val bobDate     = df.parseDateTime("2014-01-01T12:00:00.000Z").toDate
  val charlesDate = df.parseDateTime("2014-01-01T12:30:00.000Z").toDate

  val features = Seq(
    Array("alice",   20,   10f, aliceDate,   aliceGeom),
    Array("bill",    21,   11f, billDate,    billGeom),
    Array("bob",     30,   12f, bobDate,     bobGeom),
    Array("charles", null, 12f, charlesDate, charlesGeom)
  ).map { entry =>
    ScalaSimpleFeature.create(sft, entry.head.toString, entry: _*)
  }

  def overlaps(r1: TestRange, r2: TestRange): Boolean = {
    TestGeoMesaDataStore.byteComparator.compare(r1.start, r2.start) match {
      case 0 => true
      case i if i < 0 => TestGeoMesaDataStore.byteComparator.compare(r1.end, r2.start) > 0
      case i if i > 0 => TestGeoMesaDataStore.byteComparator.compare(r2.end, r1.start) > 0
    }
  }

  "AttributeIndex" should {
    "convert shorts to bytes and back" in {
      forall(Seq(0, 32, 127, 128, 129, 255, 256, 257)) { i =>
        val bytes = AttributeIndex.indexToBytes(i)
        bytes must haveLength(2)
        val recovered = AttributeIndex.bytesToIndex(bytes(0), bytes(1))
        recovered mustEqual i
      }
    }

    "correctly set secondary index ranges" in {
      import Transaction.{AUTO_COMMIT => AC}

      val ds = new TestGeoMesaDataStore(true)
      ds.createSchema(sft)

      WithClose(ds.getFeatureWriterAppend(typeName, AC)) { writer =>
        features.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
          writer.write()
        }
      }

      def execute(filter: String, explain: Explainer = ExplainNull): Seq[String] = {
        val q = new Query(typeName, ECQL.toFilter(filter))
        // validate that ranges do not overlap
        foreach(ds.getQueryPlan(q, explainer = explain)) { qp =>
          implicit val ordering = Ordering.comparatorToOrdering(TestGeoMesaDataStore.byteComparator)
          val ranges = qp.asInstanceOf[TestQueryPlan].ranges.sortBy(_.start)
          forall(ranges.sliding(2)) { case Seq(left, right) => overlaps(left, right) must beFalse }
        }
        SelfClosingIterator(ds.getFeatureReader(q, AC)).map(_.getID).toSeq
      }

      // height filter matches bob and charles, st filters only match bob
      // this filter illustrates the overlapping range bug GEOMESA-1902
      val stFilter = "bbox(geom, 46.9, 48.9, 48.1, 49.1) AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z"

      // expect z3 ranges with the attribute equals prefix
      val results = execute(s"height = 12.0 AND $stFilter")
      results must haveLength(1)
      results must contain("bob")
    }
  }
}
