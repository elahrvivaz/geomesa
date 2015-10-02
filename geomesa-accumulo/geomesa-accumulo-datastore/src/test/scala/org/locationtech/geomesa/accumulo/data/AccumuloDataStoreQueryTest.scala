/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data

import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data._
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.{DefaultFeatureCollection, NameImpl}
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.Converters
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.{ExplainString, QueryPlanner}
import org.locationtech.geomesa.accumulo.iterators.{BinAggregatingIterator, TestData}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction}
import org.locationtech.geomesa.utils.filters.Filters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.time.Duration

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreQueryTest extends Specification with TestWithMultipleSfts {

  sequential

  val ff = CommonFactoryFinder.getFilterFactory2

  "AccumuloDataStore" should {
    "return an empty iterator correctly" in {
      val sft = createNewSchema("name:String,geom:Point:srid=4326,dtg:Date")
      val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      val builder = new SimpleFeatureBuilder(sft)
      builder.addAll(List("testType", geom, null))
      val liveFeature = builder.buildFeature("fid-1")

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

      val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)

      featureCollection.add(liveFeature)

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)
      "after writing 1 feature" >> { res should haveLength(1) }

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val cqlFilter = CQL.toFilter(s"BBOX(geom, 64.9,68.9,65.1,69.1)")
      val query = new Query(sft.getTypeName, cqlFilter)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features

      "where schema matches" >> { results.getSchema mustEqual sft }
      "and there are no results" >> { features.hasNext must beFalse }
    }

    "process a DWithin query correctly" in {
      // create the data store
      val sft = createNewSchema("name:String,geom:Point:srid=4326,dtg:Date")

      addFeature(sft, ScalaSimpleFeature.create(sft, "fid-1", "name1", "POINT(45 49)", "2010-05-07T00:00:00.000Z"))

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val geomFactory = JTSFactoryFinder.getGeometryFactory
      val q = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate(45.000001, 48.99999))), 100.0, "meters")
      val query = new Query(sft.getTypeName, q)

      // Let's read out what we wrote.
      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(query)
      val features = results.features

      "with correct result" >> {
        features.hasNext must beTrue
        features.next().getID mustEqual "fid-1"
        features.hasNext must beFalse
      }
    }

    "process a DWithin of a Linestring and dtg During query correctly" >> {
      val lineOfBufferCoords: Array[Coordinate] = Array(new Coordinate(-45, 0), new Coordinate(-90, 45))
      val geomFactory = JTSFactoryFinder.getGeometryFactory

      // create the data store
      val sftPoints = createNewSchema("*geom:Point:srid=4326,dtg:Date")

      // add the 150 excluded points
      TestData.excludedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(sftPoints, ScalaSimpleFeature.create(sftPoints, s"exfid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // add the 50 included points
      TestData.includedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(sftPoints, ScalaSimpleFeature.create(sftPoints, "infid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // compose the query
      val start   = new DateTime(2014, 6, 7, 11, 0, 0, DateTimeZone.forID("UTC"))
      val end     = new DateTime(2014, 6, 7, 13, 0, 0, DateTimeZone.forID("UTC"))
      val during  = ff.during(ff.property("dtg"), Filters.dts2lit(start, end))

      "with correct result when using a dwithin of degrees" >> {
        val dwithinUsingDegrees = ff.dwithin(ff.property("geom"),
          ff.literal(geomFactory.createLineString(lineOfBufferCoords)), 1.0, "degrees")
        val filterUsingDegrees  = ff.and(during, dwithinUsingDegrees)
        val queryUsingDegrees   = new Query(sftPoints.getTypeName, filterUsingDegrees)
        val resultsUsingDegrees = ds.getFeatureSource(sftPoints.getTypeName).getFeatures(queryUsingDegrees)
        resultsUsingDegrees.features.length mustEqual 50
      }.pendingUntilFixed("Fixed Z3 'During And Dwithin' queries for a buffer created with unit degrees")

      "with correct result when using a dwithin of meters" >> {
        val dwithinUsingMeters = ff.dwithin(ff.property("geom"),
          ff.literal(geomFactory.createLineString(lineOfBufferCoords)), 150000, "meters")
        val filterUsingMeters  = ff.and(during, dwithinUsingMeters)
        val queryUsingMeters   = new Query(sftPoints.getTypeName, filterUsingMeters)
        val resultsUsingMeters = ds.getFeatureSource(sftPoints.getTypeName).getFeatures(queryUsingMeters)
        resultsUsingMeters.features.length mustEqual 50
      }
    }

    "handle bboxes without property name" in {
      val sft = createNewSchema("name:String,geom:Point:srid=4326,dtg:Date")

      addFeature(sft, ScalaSimpleFeature.create(sft, "fid-1", "name1", "POINT(42 52)", "2010-05-07T00:00:00.000Z"))

      val filterNull = ff.bbox(ff.property(null.asInstanceOf[String]), 40, 44, 50, 54, "EPSG:4326")
      val filterEmpty = ff.bbox(ff.property(""), 40, 44, 50, 54, "EPSG:4326")
      val queryNull = new Query(sft.getTypeName, filterNull)
      val queryEmpty = new Query(sft.getTypeName, filterEmpty)

      val explainNull = {
        val o = new ExplainString
        ds.explainQuery(queryNull, o)
        o.toString()
      }
      val explainEmpty = {
        val o = new ExplainString
        ds.explainQuery(queryEmpty, o)
        o.toString()
      }

      explainNull must contain("Geometry filters: BBOX(geom, 40.0,44.0,50.0,54.0)")
      explainEmpty must contain("Geometry filters: BBOX(geom, 40.0,44.0,50.0,54.0)")

      val featuresNull = ds.getFeatureSource(sft.getTypeName).getFeatures(queryNull).features.toSeq.map(_.getID)
      val featuresEmpty = ds.getFeatureSource(sft.getTypeName).getFeatures(queryEmpty).features.toSeq.map(_.getID)

      featuresNull mustEqual Seq("fid-1")
      featuresEmpty mustEqual Seq("fid-1")
    }

    "process an OR query correctly obeying inclusion-exclusion principle" >> {
      val sft = createNewSchema("name:String,geom:Point:srid=4326,dtg:Date")

      val randVal: (Double, Double) => Double = {
        val r = new Random(System.nanoTime())
        (low, high) => {
          (r.nextDouble() * (high - low)) + low
        }
      }
      val features = (0 until 1000).map { i =>
        val lat = randVal(-0.001, 0.001)
        val lon = randVal(-0.001, 0.001)
        ScalaSimpleFeature.create(sft, s"fid-$i", "testType", s"POINT($lat $lon)")
      }
      addFeatures(sft, features)

      val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]

      val geomFactory = JTSFactoryFinder.getGeometryFactory
      val urq = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate( 0.0005,  0.0005))), 150.0, "meters")
      val llq = ff.dwithin(ff.property("geom"),
        ff.literal(geomFactory.createPoint(new Coordinate(-0.0005, -0.0005))), 150.0, "meters")
      val orq = ff.or(urq, llq)
      val andq = ff.and(urq, llq)
      val urQuery  = new Query(sft.getTypeName,  urq)
      val llQuery  = new Query(sft.getTypeName,  llq)
      val orQuery  = new Query(sft.getTypeName,  orq)
      val andQuery = new Query(sft.getTypeName, andq)

      val urNum  = fs.getFeatures(urQuery).features.length
      val llNum  = fs.getFeatures(llQuery).features.length
      val orNum  = fs.getFeatures(orQuery).features.length
      val andNum = fs.getFeatures(andQuery).features.length

      (urNum + llNum) mustEqual (orNum + andNum)
    }

    "handle between intra-day queries" in {
      val sft = createNewSchema("name:String,geom:Point:srid=4326,dtg:Date")

      addFeature(sft, ScalaSimpleFeature.create(sft, "fid-2", "name", "POINT(50.0 49.0)", "2014-01-01T12:30:00.000Z"))

      val filter =
        CQL.toFilter("bbox(geom,40,40,60,60) AND dtg BETWEEN '2014-01-01T12:00:00.000Z' AND '2014-01-01T13:00:00.000Z'")
      val query = new Query(sft.getTypeName, filter)

      // Let's read out what we wrote.
      val features = ds.getFeatureSource(sft.getTypeName).getFeatures(query).features
      features.hasNext must beTrue
      val f = features.next()
      DataUtilities.encodeFeature(f) mustEqual "fid-2=name|POINT (50 49)|2014-01-01T12:30:00.000Z"
      features.hasNext must beFalse
    }

    "handle requests with namespaces" in {
      // create the data store
      val ns = "mytestns"
      val typeName = "namespacetest"
      val sft = SimpleFeatureTypes.createType(ns, typeName, "name:String,geom:Point:srid=4326,dtg:Date")
      ds.createSchema(sft)

      val schemaWithoutNs = ds.getSchema(sft.getTypeName)

      schemaWithoutNs.getName.getNamespaceURI must beNull
      schemaWithoutNs.getName.getLocalPart mustEqual sft.getTypeName

      val schemaWithNs = ds.getSchema(new NameImpl(ns, sft.getTypeName))

      schemaWithNs.getName.getNamespaceURI mustEqual ns
      schemaWithNs.getName.getLocalPart mustEqual sft.getTypeName
    }

    "handle IDL correctly" in {
      val sft = createNewSchema(TestData.getTypeSpec())

      val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]
      val featureCollection = new DefaultFeatureCollection()
      featureCollection.addAll(TestData.allThePoints.map(TestData.createSF))
      fs.addFeatures(featureCollection)

      val srs = CRS.toSRS(DefaultGeographicCRS.WGS84)
      "default layer preview, bigger than earth, multiple IDL-wrapping geoserver BBOX" in {
        val spatial = ff.bbox("geom", -230, -110, 230, 110, srs)
        val query = new Query(sft.getTypeName, spatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 361
      }

      "greater than 180 lon diff non-IDL-wrapping geoserver BBOX" in {
        val spatial = ff.bbox("geom", -100, 1.1, 100, 4.1, srs)
        val query = new Query(sft.getTypeName, spatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 6
      }

      "small IDL-wrapping geoserver BBOXes" in {
        val spatial1 = ff.bbox("geom", -181.1, -90, -175.1, 90, srs)
        val spatial2 = ff.bbox("geom", 175.1, -90, 181.1, 90, srs)
        val binarySpatial = ff.or(spatial1, spatial2)
        val query = new Query(sft.getTypeName, binarySpatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 10
      }

      "large IDL-wrapping geoserver BBOXes" in {
        val spatial1 = ff.bbox("geom", -181.1, -90, 40.1, 90, srs)
        val spatial2 = ff.bbox("geom", 175.1, -90, 181.1, 90, srs)
        val binarySpatial = ff.or(spatial1, spatial2)
        val query = new Query(sft.getTypeName, binarySpatial)
        val results = fs.getFeatures(query)
        results.size() mustEqual 226
      }
    }

    "support bin queries" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val sft = createNewSchema(s"name:String,dtg:Date,*geom:Point:srid=4326")

      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", "2010-05-07T00:00:00.000Z", "POINT(45 45)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", "2010-05-07T01:00:00.000Z", "POINT(45 45)"))

      val query = new Query(sft.getTypeName, ECQL.toFilter("BBOX(geom,40,40,50,50)"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      val queryPlanner = new QueryPlanner(sft, ds.getFeatureEncoding(sft),
        ds.getIndexSchemaFmt(sft.getTypeName), ds, ds.strategyHints(sft))
      val results = queryPlanner.runQuery(query, Some(StrategyType.ST)).map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toSeq
      forall(results)(_ must beAnInstanceOf[Array[Byte]])
      val bins = results.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(2)
      bins.map(_.trackId) must containAllOf(Seq("name1", "name2").map(_.hashCode.toString))
    }

    "support bin queries with linestrings" in {
      import BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
      val sft = createNewSchema(s"name:String,dtgs:List[Date],dtg:Date,*geom:LineString:srid=4326")
      val dtgs1 = new java.util.ArrayList[Date]
      dtgs1.add(Converters.convert("2010-05-07T00:00:00.000Z", classOf[Date]))
      dtgs1.add(Converters.convert("2010-05-07T00:01:00.000Z", classOf[Date]))
      dtgs1.add(Converters.convert("2010-05-07T00:02:00.000Z", classOf[Date]))
      dtgs1.add(Converters.convert("2010-05-07T00:03:00.000Z", classOf[Date]))
      val dtgs2 = new java.util.ArrayList[Date]
      dtgs2.add(Converters.convert("2010-05-07T01:00:00.000Z", classOf[Date]))
      dtgs2.add(Converters.convert("2010-05-07T01:01:00.000Z", classOf[Date]))
      dtgs2.add(Converters.convert("2010-05-07T01:02:00.000Z", classOf[Date]))
      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", dtgs1, "2010-05-07T00:00:00.000Z", "LINESTRING(40 41, 42 43, 44 45, 46 47)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", dtgs2, "2010-05-07T01:00:00.000Z", "LINESTRING(50 50, 51 51, 52 52)"))

      val query = new Query(sft.getTypeName, ECQL.toFilter("BBOX(geom,40,40,55,55)"))
      query.getHints.put(BIN_TRACK_KEY, "name")
      query.getHints.put(BIN_DTG_KEY, "dtgs")

      val bytes = ds.getFeatureSource(sft.getTypeName).getFeatures(query).features().map(_.getAttribute(BIN_ATTRIBUTE_INDEX)).toList
      forall(bytes)(_ must beAnInstanceOf[Array[Byte]])
      val bins = bytes.flatMap(_.asInstanceOf[Array[Byte]].grouped(16).map(Convert2ViewerFunction.decode))
      bins must haveSize(7)
      val sorted = bins.sortBy(_.dtg)
      sorted(0) mustEqual BasicValues(41, 40, dtgs1(0).getTime, "name1".hashCode.toString)
      sorted(1) mustEqual BasicValues(43, 42, dtgs1(1).getTime, "name1".hashCode.toString)
      sorted(2) mustEqual BasicValues(45, 44, dtgs1(2).getTime, "name1".hashCode.toString)
      sorted(3) mustEqual BasicValues(47, 46, dtgs1(3).getTime, "name1".hashCode.toString)
      sorted(4) mustEqual BasicValues(50, 50, dtgs2(0).getTime, "name2".hashCode.toString)
      sorted(5) mustEqual BasicValues(51, 51, dtgs2(1).getTime, "name2".hashCode.toString)
      sorted(6) mustEqual BasicValues(52, 52, dtgs2(2).getTime, "name2".hashCode.toString)
    }

    "kill queries after a configurable timeout" in {
      val params = Map(
        "connector" -> ds.connector,
        "tableName" -> ds.catalogTable,
        AccumuloDataStoreFactory.params.queryTimeoutParam.getName -> "1"
      )

      val dsWithTimeout = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      val sft = createNewSchema(s"name:String,dtg:Date,*geom:Point:srid=4326")

      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", "2010-05-07T00:00:00.000Z", "POINT(45 55)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", "2010-05-07T01:00:00.000Z", "POINT(45 55)"))

      val reader = dsWithTimeout.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT)
      reader.isClosed must beFalse
      reader.isClosed must eventually(10, new Duration(1000))(beTrue) // reaper thread runs every 5 seconds
    }

    "allow query strategy to be specified via view params" in {
      val sft = createNewSchema(s"name:String:index=join,dtg:Date,*geom:Point:srid=4326")

      addFeature(sft, ScalaSimpleFeature.create(sft, "1", "name1", "2010-05-07T00:00:00.000Z", "POINT(45 45)"))
      addFeature(sft, ScalaSimpleFeature.create(sft, "2", "name2", "2010-05-07T01:00:00.000Z", "POINT(45 45)"))

      val query = new Query(sft.getTypeName, ECQL.toFilter("BBOX(geom,40,40,50,50) and name='name1'"))

      def expectStrategy(strategy: String) = {
        val explain = new ExplainString
        ds.explainQuery(query, explain)
        explain.toString().split("\n").filter(_.startsWith("Strategy:")) mustEqual Array(s"Strategy: $strategy")
      }

      query.getHints.put(QUERY_STRATEGY_KEY, StrategyType.ATTRIBUTE)
      expectStrategy("AttributeIdxStrategy")

      query.getHints.put(QUERY_STRATEGY_KEY, StrategyType.ST)
      expectStrategy("STIdxStrategy")

      val viewParams =  new java.util.HashMap[String, String]
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, viewParams)

      query.getHints.remove(QUERY_STRATEGY_KEY)
      viewParams.put("STRATEGY", "attribute")
      expectStrategy("AttributeIdxStrategy")

      query.getHints.remove(QUERY_STRATEGY_KEY)
      viewParams.put("STRATEGY", "ST")
      expectStrategy("STIdxStrategy")

      success
    }
  }
}
