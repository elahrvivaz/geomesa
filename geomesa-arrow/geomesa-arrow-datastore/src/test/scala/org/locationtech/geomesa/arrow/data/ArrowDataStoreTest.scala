/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import java.io.File
import java.nio.file.Files

import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowDataStoreTest extends Specification {

  import scala.collection.JavaConversions._

  val sft = SimpleFeatureTypes.createType("test", "name:String,foo:String,dtg:Date,*geom:Point:srid=4326")

  val features0 = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"foo${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }
  val features1 = (10 until 20).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"foo${i % 3}", s"2017-03-15T00:$i:00.000Z", s"POINT (4${i -10} 5${i -10})")
  }
  val features = features0 ++ features1

  // note: need to compare as we iterate since values are only valid until 'next'
  def compare(features: Iterator[SimpleFeature], expected: Seq[SimpleFeature]): MatchResult[Any] = {
    var i = 0
    while (features.hasNext) {
      features.next mustEqual expected(i)
      i += 1
    }
    i mustEqual expected.length
  }

  "ArrowDataStore" should {
    "write and read values" >> {
      val file = Files.createTempFile("gm-arrow-ds", ".arrow").toUri.toURL
      try {
        val ds = DataStoreFinder.getDataStore(Map("url" -> file))
        ds must not(beNull)

        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        var caching = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> true))
        caching.getSchema(sft.getTypeName) mustEqual sft
        caching.dispose() must not(throwAn[Exception])

        var writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        features0.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, overrideFid = true)
          writer.write()
        }
        writer.close()

        caching = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> true))

        foreach(Seq(ds, caching, caching)) { store =>
          val results = CloseableIterator(store.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))
          try {
            compare(results, features0)
          } finally {
            results.close()
          }
        }

        caching.dispose() must not(throwAn[Exception])

        writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        features1.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, overrideFid = true)
          writer.write()
        }
        writer.close()

        caching = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> true))

        foreach(Seq(ds, caching, caching)) { store =>
          val results = CloseableIterator(store.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))
          try {
            compare(results, features0 ++ features1)
          } finally {
            results.close()
          }
        }

        caching.dispose() must not(throwAn[Exception])

        ds.dispose() must not(throwAn[Exception])
      } finally {
        if (!new File(file.getPath).delete()) {
          new File(file.getPath).deleteOnExit()
        }
      }
    }

    "read and filter different files" >> {
      val sftName = "test"
      val queries = Seq(
        "INCLUDE",
        "foo = 'foo1'",
        "bbox(geom, 35, 45, 45, 55)",
        "bbox(geom, 35, 45, 45, 55) and dtg DURING 2017-03-15T00:00:00.000Z/2017-03-15T00:03:00.000Z"
      ).map(ecql => new Query(sftName, ECQL.toFilter(ecql)))

      "only schema" >> {
        val file = getClass.getClassLoader.getResource("data/empty.arrow").toString
        foreach(Seq(true, false)) { caching =>
          var ds = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> caching))
          ds.getSchema("test") mustEqual sft
          WithClose(ds.getFeatureSource(sftName).getFeatures().features())(_.hasNext must beFalse)
          ds.dispose() must not(throwAn[Exception])
        }
      }

      "simple 2 batches" >> {
        val file = getClass.getClassLoader.getResource("data/simple.arrow").toString
        foreach(Seq(true, false)) { caching =>
          var ds = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> caching))
          ds.getSchema(sftName) mustEqual sft
          foreach(queries) { query =>
            WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
              compare(results, features.filter(query.getFilter.evaluate))
            }
          }
          ds.dispose() must not(throwAn[Exception])
        }
      }

      "multiple logical files" >> {
        val file = getClass.getClassLoader.getResource("data/multi-files.arrow").toString
        foreach(Seq(true, false)) { caching =>
          var ds = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> caching))
          ds.getSchema(sftName) mustEqual sft
          foreach(queries) { query =>
            WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
              compare(results, features.filter(query.getFilter.evaluate))
            }
          }
          ds.dispose() must not(throwAn[Exception])
        }
      }

      "dictionary encoded files" >> {
        val file = getClass.getClassLoader.getResource("data/dictionaries.arrow").toString
        foreach(Seq(true, false)) { caching =>
          var ds = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> caching))
          ds.getSchema(sftName) mustEqual sft
          foreach(queries) { query =>
            WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
              compare(results, features.filter(query.getFilter.evaluate))
            }
          }
          ds.dispose() must not(throwAn[Exception])
        }
      }

      "dictionary encoded files with default values" >> {
        val file = getClass.getClassLoader.getResource("data/dictionary-defaults.arrow").toString
        // the file has only 'foo0' and 'foo1' encoded
        val dictionaryFeatures = features.map {
          case f if f.getAttribute("foo") != "foo2" => f
          case f =>
            val updated = ScalaSimpleFeature.create(f)
            updated.setAttribute("foo", "[other]")
            updated
        }
        foreach(Seq(true, false)) { caching =>
          var ds = DataStoreFinder.getDataStore(Map("url" -> file, "caching" -> caching))
          ds.getSchema(sftName) mustEqual sft
          foreach(queries) { query =>
            WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
              results.hasNext must beTrue // just check our filter was valid
              compare(results, dictionaryFeatures.filter(query.getFilter.evaluate))
            }
          }
          ds.dispose() must not(throwAn[Exception])
        }
      }
    }
  }
}
