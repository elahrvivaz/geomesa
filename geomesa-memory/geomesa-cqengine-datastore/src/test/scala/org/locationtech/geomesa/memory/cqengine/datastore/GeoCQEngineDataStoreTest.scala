/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import org.geotools.data.{DataStoreFinder, DataUtilities, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.referencing.GeodeticCalculator
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Point
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoCQEngineDataStoreTest extends Specification {

  sequential

  "GeoCQEngineData" should {

    val params = Map("cqengine" -> "true")
    val ds = DataStoreFinder.getDataStore(params)

    "get a datastore" in {
      ds mustNotEqual null
    }

    "createSchema" in {
      ds.createSchema(SampleFeatures.sft)
      ds.getTypeNames.length mustEqual 1
      ds.getTypeNames.contains("test") mustEqual true
    }

    "insert features" in {
      val fs = ds.getFeatureSource("test").asInstanceOf[GeoCQEngineFeatureStore]
      fs must not(beNull)
      fs.addFeatures(DataUtilities.collection(SampleFeatures.feats))
      fs.getCount(Query.ALL) mustEqual 1000
      SelfClosingIterator(fs.getFeatures().features()).map(_.getID).toList.sorted mustEqual
        SampleFeatures.feats.map(_.getID).sorted
    }

    "not allow feature modification" in {
      val query = new Query("test", ECQL.toFilter("IN ('1')"))
      val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      result must haveLength(1)
      result.head.setAttribute(0, "update") must throwAn[UnsupportedOperationException]
    }

    "work with dwithin" in {
      val point = WKTUtils.read("POINT (-23.031561371563157 -60.56546392759515)").asInstanceOf[Point]
      val dist = 10000d
      val filter = FastFilterFactory.toFilter(SampleFeatures.sft, s"dwithin(Where,$point,$dist,meters)")
      val expected = SampleFeatures.feats.filter(filter.evaluate)
      expected must haveLength(2)
      val query = new Query("test", filter)
      val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      result must haveLength(2)
      result must containTheSameElementsAs(expected)
      foreach(result) { sf =>
        val resultPoint = sf.getDefaultGeometry.asInstanceOf[Point]
        val calc = new GeodeticCalculator()
        calc.setStartingGeographicPoint(point.getX, point.getY)
        calc.setDestinationGeographicPoint(resultPoint.getX, resultPoint.getY)
        calc.getOrthodromicDistance must beLessThan(dist)
      }
    }
  }
}
