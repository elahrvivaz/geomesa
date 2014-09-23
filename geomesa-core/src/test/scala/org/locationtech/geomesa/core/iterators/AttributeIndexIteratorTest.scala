/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriterConfig, IteratorSetting}
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.util.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AttributeIndexIteratorTest extends Specification {
sequential
  val sftName = "AttributeIndexIteratorTest"
  val spec = "name:String:index=true,age:Integer:index=true,dtg:Date:index=true,*geom:Geometry:srid=4326"
  val sft = SimpleFeatureTypes.createType(sftName, spec)
  index.setDtgDescriptor(sft, "dtg")

  val sdf = new SimpleDateFormat("yyyyMMdd")
  sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))
  val dateToIndex = sdf.parse("20140102")

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> "AttributeIndexIteratorTest",
      "useMock"           -> "true")).asInstanceOf[AccumuloDataStore]

  val ds = createStore

  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  List("a", "b", "c", "d").foreach { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
      val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute("dtg", dateToIndex)
      sf.setAttribute("age", i)
      sf.setAttribute("name", name)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
  }

  fs.addFeatures(featureCollection)
  fs.flush()

  val ff = CommonFactoryFinder.getFilterFactory2

  "AttributeIndexIterator" should {

    "implement the Accumulo iterator stack properly" in {
      skipped("skipped")
      val table = "AttributeIndexIteratorTest_2"
      val instance = new MockInstance(table)
      val conn = instance.getConnector("", new PasswordToken(""))
      conn.tableOperations.create(table, true, TimeType.LOGICAL)

      val bw = conn.createBatchWriter(table, new BatchWriterConfig)
      featureCollection.foreach { feature =>
        val muts = AttributeTable.getAttributeIndexMutations(feature,
                                                             sft.getAttributeDescriptors,
                                                             new ColumnVisibility(), "")
        bw.addMutations(muts)
      }
      bw.close()

      // Scan and retrieve type = b manually with the iterator
      val scanner = conn.createScanner(table, new Authorizations())
      val opts = Map[String, String](GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE -> spec)
      val is = new IteratorSetting(40, classOf[AttributeIndexIterator], opts)
      scanner.addScanIterator(is)
      scanner.setRange(new ARange(AttributeTable.getAttributeIndexRow("", "name", Some("b"))))
      scanner.iterator.size mustEqual 4
    }

    "be selected for appropriate queries" in {
      skipped("skipped")
      List("name = 'b'",
           "name < 'b'",
           "name > 'b'",
           "name is NULL",
           "dtg TEQUALS 2014-01-01T12:30:00.000Z",
           "dtg = '2014-01-01T12:30:00.000Z'",
           "dtg BETWEEN '2012-01-01T12:00:00.000Z' AND '2013-01-01T12:00:00.000Z'",
           "age < 10"
          ).foreach { filter =>
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val explain = new ExplainString()
        ds.getFeatureReader(sftName, query).explainQuery(o = explain)
        val output = explain.toString()
        println("EXPLAIN:: " + output)
        println
        val iter = output.split("\n").filter(_.startsWith("addScanIterator")).headOption
        iter.isDefined mustEqual true
        iter.get must contain(classOf[AttributeIndexIterator].getName)
      }
      success
    }
//TODO also check extra filters
    "return correct results" >> {

      "for string equals" >> {
        val filter = "name = 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList

        results must haveSize(4)
        results.map(_.getAttributeCount) must contain(3).foreach
//        results.map(_.getAttribute("name").asInstanceOf[String]) must contain("b").foreach
        results.map(_.getAttribute("geom").toString) must contain("POINT (45 45)", "POINT (46 46)", "POINT (47 47)", "POINT (48 48)")
        results.map(_.getAttribute("dtg").asInstanceOf[Date]) must contain(dateToIndex).foreach
      }

      "for string less than" >> {
        skipped("skipped")
        val filter = "name < 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for string greater than" >> {
        skipped("skipped")
        val filter = "name > 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for string greater than or equals" >> {
        skipped("skipped")
        val filter = "name >= 'b'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for string null" >> {
        skipped("skipped")
        val filter = "name is NULL"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "name"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for date tequals" >> {
        skipped("skipped")
        val filter = "dtg TEQUALS 2014-01-01T12:30:00.000Z"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for date equals" >> {
        skipped("skipped")
        val filter = "dtg = '2014-01-01T12:30:00.000Z'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for date between" >> {
        skipped("skipped")
        val filter = "dtg BETWEEN '2012-01-01T12:00:00.000Z' AND '2013-01-01T12:00:00.000Z'"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for int less than" >> {
        skipped("skipped")
        val filter = "age < 10"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "age"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }

      "for int greater than or equals" >> {
        skipped("skipped")
        val filter = "age >= 10"
        val query = new Query(sftName, ECQL.toFilter(filter), Array("geom", "dtg", "age"))
        val results = SelfClosingIterator(CloseableIterator(ds.getFeatureReader(sftName, query))).toList
        success
      }
    }

  }

}
