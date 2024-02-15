/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.{TestWithFeatureType, TestWithMultipleSfts}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.Z2Iterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.AtomicWritesTransaction
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.locationtech.geomesa.utils.text.{StringSerialization, WKTUtils}
import org.locationtech.jts.geom.{Geometry, Point}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.IOException
import java.util.Date

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAtomicWriteTest extends Specification with TestWithFeatureType {

  import scala.collection.JavaConverters._

  sequential

  override val spec = "name:String:index=true,dtg:Date,geom:Point:srid=4326"

  lazy val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"2024-02-15T0$i:00:01.000Z", s"POINT (0 $i)")
  }
  lazy val update4 = ScalaSimpleFeature.create(sft, "4", "name4", "2024-02-15T04:00:02.000Z", "POINT (1 4)")

  val filters = Seq(
    s"IN(${Seq.tabulate(10)(i => i).mkString("'", "','", "'")})", // id index
    "bbox(geom, -1, -1, 10, 10)", // z2
    "bbox(geom, -1, -1, 10, 10) AND dtg during 2024-02-15T00:00:00.000Z/2024-02-15T12:00:00.000Z", // z3
    s"name IN(${Seq.tabulate(10)(i => s"name$i").mkString("'", "','", "'")})" // attribute
  ).map(ECQL.toFilter)

  def query(filter: Filter): Seq[SimpleFeature] = {
    val query = new Query(sft.getTypeName, filter)
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
  }

  "AccumuloDataStore" should {
    "use an atomic writer" in {
      WithClose(ds.getFeatureWriterAppend(sftName, AtomicWritesTransaction.INSTANCE)) { writer =>
        features.take(5).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
      foreach(filters) { filter =>
        query(filter) mustEqual features.take(5)
      }
    }
    "make updates" in {
      WithClose(ds.getFeatureWriter(sftName, ECQL.toFilter("IN ('4')"), AtomicWritesTransaction.INSTANCE)) { writer =>
        writer.hasNext must beTrue
        val update = writer.next
        update.setAttribute("geom", update4.getAttribute("geom"))
        update.setAttribute("dtg", update4.getAttribute("dtg"))
        writer.write()
      }
      foreach(filters) { filter =>
        query(filter) mustEqual features.take(4) ++ Seq(update4)
      }
      // revert back to the original feature
      WithClose(ds.getFeatureWriter(sftName, ECQL.toFilter("IN ('4')"), AtomicWritesTransaction.INSTANCE)) { writer =>
        writer.hasNext must beTrue
        val update = writer.next
        update.setAttribute("geom", features(4).getAttribute("geom"))
        update.setAttribute("dtg", features(4).getAttribute("dtg"))
        writer.write()
      }
      foreach(filters) { filter =>
        query(filter) mustEqual features.take(5)
      }
    }
  }
}
