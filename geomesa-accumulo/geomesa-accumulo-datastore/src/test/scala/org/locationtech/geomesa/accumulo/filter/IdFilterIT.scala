/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.filter

import java.util.Date

import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.jts.geom.Coordinate
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IdFilterIT extends TestWithFeatureType {

  import org.locationtech.geomesa.filter.ff

  override val spec = "age:Int:index=join,name:String:index=join,dtg:Date,*geom:Point:srid=4326"

  val geomBuilder = JTSFactoryFinder.getGeometryFactory
  val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
  val data = List(
    ("1", Array(10, "johndoe", new Date), geomBuilder.createPoint(new Coordinate(10, 10))),
    ("2", Array(20, "janedoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20))),
    ("3", Array(30, "johnrdoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20)))
  )
  val features = data.map { case (id, attrs, geom) =>
    builder.reset()
    builder.addAll(attrs.asInstanceOf[Array[AnyRef]])
    val f = builder.buildFeature(id)
    f.setDefaultGeometry(geom)
    f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    f
  }

  step {
    addFeatures(features)
  }

  lazy val fs = ds.getFeatureSource(sftName)

  "Id queries" should {
    "use record table to return a result" >> {
      val idQ = ff.id(ff.featureId("2"))
      val res = SelfClosingIterator(fs.getFeatures(idQ).features).toList
      res.length mustEqual 1
      res.head.getID mustEqual "2"
    }

    "handle multiple ids correctly" >> {
      val idQ = ff.id(ff.featureId("1"), ff.featureId("3"))
      val res = SelfClosingIterator(fs.getFeatures(idQ).features).toList
      res.length mustEqual 2
      res.map(_.getID) must contain ("1", "3")
    }

    "return no events when multiple IDs ANDed result in no intersection"  >> {
      val idQ1 = ff.id(ff.featureId("1"), ff.featureId("3"))
      val idQ2 = ff.id(ff.featureId("2"))
      val idQ =  ff.and(idQ1, idQ2)
      val qRes = fs.getFeatures(idQ)
      val res= SelfClosingIterator(qRes.features).toList
      res.length mustEqual 0
    }
  }
}
