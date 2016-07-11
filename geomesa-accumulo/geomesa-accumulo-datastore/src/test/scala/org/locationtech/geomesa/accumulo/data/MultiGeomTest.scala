/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

class MultiGeomTest extends TestWithDataStore {

  override val spec = "track:String,dtg:Date,projected:Point:srid=4326,*geom:Point:srid=4326"

  addFeatures((0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(i.toString, sft)
    sf.setAttribute(0, i.toString)
    sf.setAttribute(1, s"2015-01-01T00:0$i:01.000Z")
    sf.setAttribute(2, s"POINT(-112.$i 45)")
    sf.setAttribute(3, s"POINT(-110.$i 45)")
    sf
  })

  "AccumuloDataStore" should {
    "support alternate geometries in BIN records" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val filter = "bbox(geom, -112, 44, -109, 46) AND dtg DURING 2015-01-01T00:00:00.000Z/2015-01-01T00:10:00.000Z"
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(QueryHints.BIN_TRACK_KEY, "track")
      query.getHints.put(QueryHints.BIN_GEOM_KEY, "projected")
      query.getHints.put(QueryHints.BIN_DTG_KEY, "dtg")
      query.getHints.put(QueryHints.BIN_BATCH_SIZE_KEY, 1000)
      val reader = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val bytes = reader.map(_.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]).toList
      val bins = bytes.flatMap(_.grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveLength(10)
      bins.map(_.trackId) must containTheSameElementsAs((0 until 10).map(_.toString.hashCode.toString))
      bins.map(_.dtg) must containTheSameElementsAs((0 until 10).map { i =>
        GeoToolsDateFormat.parseMillis(s"2015-01-01T00:0$i:01.000Z")
      })
      // compare strings so we don't get double comparison errors
      bins.map(_.lat.toString) must contain(beEqualTo("45.0")).forall
      bins.map(_.lon.toString) must containTheSameElementsAs((0 until 10).map(i => s"-112.$i"))
    }
  }
}
