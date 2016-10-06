/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.cql2.CQLException
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.attribute.{AttributeIndex, AttributeWritableIndex}
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.index.api.{FilterSplitter, FilterStrategy}
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AttributeIndexStrategyTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "name:String:index=full,age:Integer:index=true,count:Long:index=true," +
      "weight:Double:index=true,height:Float:index=true,admin:Boolean:index=true," +
      "geom:Point:srid=4326,dtg:Date,indexedDtg:Date:index=true,fingers:List[String]:index=true," +
      "toes:List[Double]:index=true,track:String,geom2:Point:srid=4326;geomesa.indexes.enabled='attr_idx,records'"

  val geom = WKTUtils.read("POINT(45.0 49.0)")
  val geom2 = WKTUtils.read("POINT(55.0 59.0)")

  val df = ISODateTimeFormat.dateTime()

  val annaDate = df.parseDateTime("2012-01-01T12:00:00.000Z").toDate
  val billDate = df.parseDateTime("2013-01-01T12:00:00.000Z").toDate
  val burtDate = df.parseDateTime("2014-01-01T12:00:00.000Z").toDate
  val carlDate = df.parseDateTime("2014-01-01T12:30:00.000Z").toDate

  val annaFingers = List("index")
  val billFingers = List("ring", "middle")
  val burtFingers = List("index", "thumb", "pinkie")
  val carlFingers = List("thumb", "ring", "index", "pinkie", "middle")

  val annaToes = List(1.0)
  val billToes = List(1.0, 2.0)
  val burtToes = List(3.0, 2.0, 5.0)
  val carlToes = List()

  val features = Seq(
    Array("anna", 20,   1, 5.0, 10.0F, true,  geom, annaDate, annaDate, annaFingers, annaToes, "track1", geom2),
    Array("bill", 21,   2, 6.0, 11.0F, false, geom, billDate, billDate, billFingers, billToes, "track2", geom2),
    Array("burt", 30,   3, 6.0, 12.0F, false, geom, burtDate, burtDate, burtFingers, burtToes, "track1", geom2),
    Array("carl", null, 4, 7.0, 12.0F, false, geom, carlDate, carlDate, carlFingers, carlToes, "track1", geom2)
  ).map { entry =>
    val feature = new ScalaSimpleFeature(entry.head.toString, sft)
    feature.setAttributes(entry.asInstanceOf[Array[AnyRef]])
    feature
  }

  addFeatures(features)

  def execute(filter: String, explain: Explainer = ExplainNull): List[String] = {
    val query = new Query(sftName, ECQL.toFilter(filter))
    forall(ds.getQueryPlan(query, explainer = explain))(_.filter.index mustEqual AttributeIndex)
    val results = SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features())
    results.map(_.getAttribute("name").toString).toList
  }

  def runQuery(query: Query): Iterator[SimpleFeature] = {
    forall(ds.getQueryPlan(query))(_.filter.index mustEqual AttributeIndex)
    SelfClosingIterator(ds.getFeatureSource(sftName).getFeatures(query).features())
  }

  "AttributeIndexStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getTableName(sftName, AttributeIndex), new Authorizations())
      val prefix = AttributeWritableIndex.getRowPrefix(sft, sft.indexOf("fingers"))
      scanner.setRange(AccRange.prefix(new Text(prefix)))
      scanner.asScala.foreach(println)
      println()
      success
    }

    "all attribute filters should be applied to SFFI" in {
      val filter = andFilters(Seq(ECQL.toFilter("name LIKE 'b%'"), ECQL.toFilter("count<27"), ECQL.toFilter("age<29")))
      val results = execute(ECQL.toCQL(filter))
      results must haveLength(1)
      results must contain ("bill")
    }

    "support bin queries with join queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[JoinPlan])
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq("bill", "burt", "carl").map(_.hashCode.toString))
    }

    "support bin queries against index values" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2"))
      query.getHints.put(BIN_TRACK_KEY, "dtg")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      forall(ds.getQueryPlan(query))(_ must not(beAnInstanceOf[JoinPlan]))
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq(billDate, burtDate, carlDate).map(_.hashCode.toString))
    }

    "support bin queries against full values" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("name>'april'"))
      query.getHints.put(BIN_TRACK_KEY, "count")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      forall(ds.getQueryPlan(query))(_ must not(beAnInstanceOf[JoinPlan]))
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq(2, 3, 4).map(_.hashCode.toString))
    }

    "support bin queries with join queries against alternate geometry" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val query = new Query(sftName, ECQL.toFilter("count>=2 AND bbox(geom2,50,50,60,60)"))
      query.getHints.put(BIN_GEOM_KEY, "geom2")
      query.getHints.put(BIN_TRACK_KEY, "name")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      forall(ds.getQueryPlan(query))(_ must beAnInstanceOf[JoinPlan])
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(3)
      bins.map(_.trackId) must containAllOf(Seq("bill", "burt", "carl").map(_.hashCode.toString))
      bins.map(_.lon) must contain(55f, 55f, 55f)
      bins.map(_.lat) must contain(59f, 59f, 59f)
    }

    "correctly query equals with date ranges" in {
      val features = execute("height = 12.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("burt")
    }

    "correctly query lt with date ranges" in {
      val features = execute("height < 12.0 AND " +
          "dtg DURING 2011-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z")
      features must haveLength(1)
      features must contain("anna")
    }

    "correctly query lte with date ranges" in {
      val features = execute("height <= 12.0 AND " +
          "dtg DURING 2013-01-01T00:00:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(2)
      features must contain("bill", "burt")
    }

    "correctly query gt with date ranges" in {
      val features = execute("height > 11.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("burt")
    }

    "correctly query gte with date ranges" in {
      val features = execute("height >= 11.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("burt")
    }

    "correctly query between with date ranges" in {
      val features = execute("height between 11.0 AND 12.0 AND " +
          "dtg DURING 2014-01-01T11:45:00.000Z/2014-01-01T12:15:00.000Z")
      features must haveLength(1)
      features must contain("burt")
    }

    "support sampling" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
    }

    "support sampling with cql" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a' AND track > 'track'"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
    }

    "support sampling with transformations" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"), Array("name", "geom"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      val results = runQuery(query).toList
      results must haveLength(2)
      forall(results)(_.getAttributeCount mustEqual 2)
    }

    "support sampling with cql and transformations" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a' AND track > 'track'"), Array("name", "geom"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.2f))
      val results = runQuery(query).toList
      results must haveLength(1)
      results.head.getAttributeCount mustEqual 2
    }

    "support sampling by thread" in {
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      query.getHints.put(SAMPLE_BY_KEY, "track")
      val results = runQuery(query).toList
      results must haveLength(2)
      results.map(_.getAttribute("track")) must containTheSameElementsAs(Seq("track1", "track2"))
    }

    "support sampling with bin queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      // important - id filters will create multiple ranges and cause multiple iterators to be created
      val query = new Query(sftName, ECQL.toFilter("name > 'a'"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      query.getHints.put(BIN_BATCH_SIZE_KEY, 1000)
      query.getHints.put(SAMPLING_KEY, new java.lang.Float(.5f))
      // have to evaluate attributes before pulling into collection, as the same sf is reused
      val results = runQuery(query).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(2)
    }
  }

  "AttributeIndexEqualsStrategy" should {

    "correctly query on ints" in {
      val features = execute("age=21")
      features must haveLength(1)
      features must contain("bill")
    }

    "correctly query on longs" in {
      val features = execute("count=2")
      features must haveLength(1)
      features must contain("bill")
    }

    "correctly query on floats" in {
      val features = execute("height=12.0")
      features must haveLength(2)
      features must contain("burt", "carl")
    }

    "correctly query on floats in different precisions" in {
      val features = execute("height=10")
      features must haveLength(1)
      features must contain("anna")
    }

    "correctly query on doubles" in {
      val features = execute("weight=6.0")
      features must haveLength(2)
      features must contain("bill", "burt")
    }

    "correctly query on doubles in different precisions" in {
      val features = execute("weight=6")
      features must haveLength(2)
      features must contain("bill", "burt")
    }

    "correctly query on booleans" in {
      val features = execute("admin=false")
      features must haveLength(3)
      features must contain("bill", "burt", "carl")
    }

    "correctly query on strings" in {
      val features = execute("name='bill'")
      features must haveLength(1)
      features must contain("bill")
    }

    "correctly query on OR'd strings" in {
      val features = execute("name = 'bill' OR name = 'carl'")
      features must haveLength(2)
      features must contain("bill", "carl")
    }

    "correctly query on IN strings" in {
      val features = execute("name IN ('bill', 'carl')")
      features must haveLength(2)
      features must contain("bill", "carl")
    }

    "correctly query on OR'd strings with bboxes" in {
      val features = execute("(name = 'bill' OR name = 'carl') AND bbox(geom,40,45,50,55)")
      features must haveLength(2)
      features must contain("bill", "carl")
    }

    "correctly query on IN strings with bboxes" in {
      val features = execute("name IN ('bill', 'carl') AND bbox(geom,40,45,50,55)")
      features must haveLength(2)
      features must contain("bill", "carl")
    }

    "correctly query on redundant OR'd strings" in {
      val features = execute("(name = 'bill' OR name = 'carl') AND name = 'carl'")
      features must haveLength(1)
      features must contain("carl")
    }

    "correctly query on date objects" in {
      val features = execute("indexedDtg TEQUALS 2014-01-01T12:30:00.000Z")
      features must haveLength(1)
      features must contain("carl")
    }

    "correctly query on date strings in standard format" in {
      val features = execute("indexedDtg = '2014-01-01T12:30:00.000Z'")
      features must haveLength(1)
      features must contain("carl")
    }

    "correctly query on lists of strings" in {
      val features = execute("fingers = 'index'")
      features must haveLength(3)
      features must contain("anna", "burt", "carl")
    }

    "correctly query on lists of doubles" in {
      val features = execute("toes = 2.0")
      features must haveLength(2)
      features must contain("bill", "burt")
    }
  }

  "AttributeIndexRangeStrategy" should {

    "correctly query on ints (with nulls)" >> {
      "lt" >> {
        val features = execute("age<21")
        features must haveLength(1)
        features must contain("anna")
      }
      "gt" >> {
        val features = execute("age>21")
        features must haveLength(1)
        features must contain("burt")
      }
      "lte" >> {
        val features = execute("age<=21")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
      "gte" >> {
        val features = execute("age>=21")
        features must haveLength(2)
        features must contain("bill", "burt")
      }
      "between (inclusive)" >> {
        val features = execute("age BETWEEN 20 AND 25")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
    }

    "correctly query on longs" >> {
      "lt" >> {
        val features = execute("count<2")
        features must haveLength(1)
        features must contain("anna")
      }
      "gt" >> {
        val features = execute("count>2")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
      "lte" >> {
        val features = execute("count<=2")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
      "gte" >> {
        val features = execute("count>=2")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("count BETWEEN 3 AND 7")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
    }

    "correctly query on floats" >> {
      "lt" >> {
        val features = execute("height<12.0")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
      "gt" >> {
        val features = execute("height>12.0")
        features must haveLength(0)
      }
      "lte" >> {
        val features = execute("height<=12.0")
        features must haveLength(4)
        features must contain("anna", "bill", "burt", "carl")
      }
      "gte" >> {
        val features = execute("height>=12.0")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("height BETWEEN 10.0 AND 11.5")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
    }

    "correctly query on floats in different precisions" >> {
      "lt" >> {
        val features = execute("height<11")
        features must haveLength(1)
        features must contain("anna")
      }
      "gt" >> {
        val features = execute("height>11")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
      "lte" >> {
        val features = execute("height<=11")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
      "gte" >> {
        val features = execute("height>=11")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("height BETWEEN 11 AND 12")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
    }

    "correctly query on doubles" >> {
      "lt" >> {
        val features = execute("weight<6.0")
        features must haveLength(1)
        features must contain("anna")
      }
      "lt fraction" >> {
        val features = execute("weight<6.1")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
      "gt" >> {
        val features = execute("weight>6.0")
        features must haveLength(1)
        features must contain("carl")
      }
      "gt fractions" >> {
        val features = execute("weight>5.9")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "lte" >> {
        val features = execute("weight<=6.0")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
      "gte" >> {
        val features = execute("weight>=6.0")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("weight BETWEEN 5.5 AND 6.5")
        features must haveLength(2)
        features must contain("bill", "burt")
      }
    }

    "correctly query on doubles in different precisions" >> {
      "lt" >> {
        val features = execute("weight<6")
        features must haveLength(1)
        features must contain("anna")
      }
      "gt" >> {
        val features = execute("weight>6")
        features must haveLength(1)
        features must contain("carl")
      }
      "lte" >> {
        val features = execute("weight<=6")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
      "gte" >> {
        val features = execute("weight>=6")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("weight BETWEEN 5 AND 6")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
    }

    "correctly query on strings" >> {
      "lt" >> {
        val features = execute("name<'bill'")
        features must haveLength(1)
        features must contain("anna")
      }
      "gt" >> {
        val features = execute("name>'bill'")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
      "lte" >> {
        val features = execute("name<='bill'")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
      "gte" >> {
        val features = execute("name>='bill'")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("name BETWEEN 'bill' AND 'burt'")
        features must haveLength(2)
        features must contain("bill", "burt")
      }
    }

    "correctly query on date objects" >> {
      "before" >> {
        val features = execute("indexedDtg BEFORE 2014-01-01T12:30:00.000Z")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
      "after" >> {
        val features = execute("indexedDtg AFTER 2013-01-01T12:30:00.000Z")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
      "during (exclusive)" >> {
        val features = execute("indexedDtg DURING 2012-01-01T11:00:00.000Z/2014-01-01T12:15:00.000Z")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
    }

    "correctly query on date strings in standard format" >> {
      "lt" >> {
        val features = execute("indexedDtg < '2014-01-01T12:30:00.000Z'")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
      "gt" >> {
        val features = execute("indexedDtg > '2013-01-01T12:00:00.000Z'")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("indexedDtg BETWEEN '2012-01-01T12:00:00.000Z' AND '2013-01-01T12:00:00.000Z'")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
    }

    "correctly query with attribute on right side" >> {
      "lt" >> {
        val features = execute("'bill' > name")
        features must haveLength(1)
        features must contain("anna")
      }
      "gt" >> {
        val features = execute("'bill' < name")
        features must haveLength(2)
        features must contain("burt", "carl")
      }
      "lte" >> {
        val features = execute("'bill' >= name")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
      "gte" >> {
        val features = execute("'bill' <= name")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "before" >> {
        execute("2014-01-01T12:30:00.000Z AFTER indexedDtg") should throwA[CQLException]
      }
      "after" >> {
        execute("2013-01-01T12:30:00.000Z BEFORE indexedDtg") should throwA[CQLException]
      }
    }

    "correctly query on lists of strings" in {
      "lt" >> {
        val features = execute("fingers<'middle'")
        features must haveLength(3)
        features must contain("anna", "burt", "carl")
      }
      "gt" >> {
        val features = execute("fingers>'middle'")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "lte" >> {
        val features = execute("fingers<='middle'")
        features must haveLength(4)
        features must contain("anna", "bill", "burt", "carl")
      }
      "gte" >> {
        val features = execute("fingers>='middle'")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
      "between (inclusive)" >> {
        val features = execute("fingers BETWEEN 'pinkie' AND 'thumb'")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }
    }

    "correctly query on lists of doubles" in {
      "lt" >> {
        val features = execute("toes<2.0")
        features must haveLength(2)
        features must contain("anna", "bill")
      }
      "gt" >> {
        val features = execute("toes>2.0")
        features must haveLength(1)
        features must contain("burt")
      }
      "lte" >> {
        val features = execute("toes<=2.0")
        features must haveLength(3)
        features must contain("anna", "bill", "burt")
      }
      "gte" >> {
        val features = execute("toes>=2.0")
        features must haveLength(2)
        features must contain("bill", "burt")
      }
      "between (inclusive)" >> {
        val features = execute("toes BETWEEN 1.5 AND 2.5")
        features must haveLength(2)
        features must contain("bill", "burt")
      }
    }

    "correctly query on not nulls" in {
      val features = execute("age IS NOT NULL")
      features must haveLength(3)
      features must contain("anna", "bill", "burt")
    }

    "correctly query on indexed attributes with nonsensical AND queries" >> {
      "redundant int query" >> {
        val features = execute("age > 25 AND age > 15")
        features must haveLength(1)
        features must contain("burt")
      }

      "int query that returns nothing" >> {
        val features = execute("age > 25 AND age < 15")
        features must haveLength(0)
      }

      "redundant float query" >> {
        val features = execute("height >= 6 AND height > 4")
        features must haveLength(4)
        features must contain("anna", "bill", "burt", "carl")
      }

      "float query that returns nothing" >> {
        val features = execute("height >= 6 AND height < 4")
        features must haveLength(0)
      }

      "redundant date query" >> {
        val features = execute("indexedDtg AFTER 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-02-01T00:00:00.000Z")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }

      "date query that returns nothing" >> {
        val features = execute("indexedDtg BEFORE 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-01-01T00:00:00.000Z")
        features must haveLength(0)
      }

      "redundant date and float query" >> {
        val features = execute("height >= 6 AND height > 4 AND indexedDtg AFTER 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-02-01T00:00:00.000Z")
        features must haveLength(3)
        features must contain("bill", "burt", "carl")
      }

      "date and float query that returns nothing" >> {
        val features = execute("height >= 6 AND height > 4 AND indexedDtg BEFORE 2011-01-01T00:00:00.000Z AND indexedDtg AFTER 2012-01-01T00:00:00.000Z")
        features must haveLength(0)
      }
    }
  }

  "AttributeIndexLikeStrategy" should {

    "correctly query on strings" in {
      val features = execute("name LIKE 'b%'")
      features must haveLength(2)
      features must contain("bill", "burt")
    }

    "correctly query on non-strings" in {
      val features = execute("age LIKE '2%'")
      features must haveLength(2)
      features must contain("anna", "bill")
    }.pendingUntilFixed("Lexicoding does not allow us to prefix search non-strings")
  }

  "AttributeIdxStrategy merging" should {
    val ff = CommonFactoryFinder.getFilterFactory2

    "merge PropertyIsEqualTo primary filters" >> {
      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val qf1 = FilterStrategy(AttributeIndex, Some(q1), None)
      val qf2 = FilterStrategy(AttributeIndex, Some(q2), None)
      val res = FilterSplitter.tryMergeAttrStrategy(qf1, qf2)
      res must not(beNull)
      res.primary must beSome(ff.or(q1, q2))
    }

    "merge PropertyIsEqualTo on multiple ORs" >> {

      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val q3 = ff.equals(ff.property("prop"), ff.literal("3"))
      val qf1 = FilterStrategy(AttributeIndex, Some(q1), None)
      val qf2 = FilterStrategy(AttributeIndex, Some(q2), None)
      val qf3 = FilterStrategy(AttributeIndex, Some(q3), None)
      val res = FilterSplitter.tryMergeAttrStrategy(FilterSplitter.tryMergeAttrStrategy(qf1, qf2), qf3)
      res must not(beNull)
      res.primary.map(decomposeOr) must beSome(containTheSameElementsAs(Seq[Filter](q1, q2, q3)))
    }

    "merge PropertyIsEqualTo when secondary matches" >> {
      val bbox = ff.bbox("geom", 1, 2, 3, 4, "EPSG:4326")
      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val q3 = ff.equals(ff.property("prop"), ff.literal("3"))
      val qf1 = FilterStrategy(AttributeIndex, Some(q1), Some(bbox))
      val qf2 = FilterStrategy(AttributeIndex, Some(q2), Some(bbox))
      val qf3 = FilterStrategy(AttributeIndex, Some(q3), Some(bbox))
      val res = FilterSplitter.tryMergeAttrStrategy(FilterSplitter.tryMergeAttrStrategy(qf1, qf2), qf3)
      res must not(beNull)
      res.primary.map(decomposeOr) must beSome(containTheSameElementsAs(Seq[Filter](q1, q2, q3)))
      res.secondary must beSome(bbox)
    }

    "not merge PropertyIsEqualTo when secondary does not match" >> {
      val bbox = ff.bbox("geom", 1, 2, 3, 4, "EPSG:4326")
      val q1 = ff.equals(ff.property("prop"), ff.literal("1"))
      val q2 = ff.equals(ff.property("prop"), ff.literal("2"))
      val qf1 = FilterStrategy(AttributeIndex, Some(q1), Some(bbox))
      val qf2 = FilterStrategy(AttributeIndex, Some(q2), None)
      val res = FilterSplitter.tryMergeAttrStrategy(qf1, qf2)
      res must beNull
    }

  }
}
