/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.locationtech.geomesa.accumulo.iterators.KryoLazyTemporalDensityIterator.{decodeTimeSeries, jsonToTimeSeries}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TemporalDensityIteratorTest extends Specification with TestWithMultipleSfts {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  def loadFeatures(sft: SimpleFeatureType, encodedFeatures: Array[_ <: Array[_]]): Unit = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

    def decodeFeature(e: Array[_]): SimpleFeature = {
      val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
      f
    }

    addFeatures(sft, encodedFeatures.map(decodeFeature))
  }

  def getQuery(sft: SimpleFeatureType, query: String): Query = {
    val q = new Query(sft.getTypeName, ECQL.toFilter(query))
    val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
    q.getHints.put(QueryHints.TEMPORAL_DENSITY_KEY, java.lang.Boolean.TRUE)
    q.getHints.put(QueryHints.TIME_INTERVAL_KEY, new Interval(new DateTime("2012-01-01T0:00:00", DateTimeZone.UTC).getMillis, new DateTime("2012-01-02T0:00:00", DateTimeZone.UTC).getMillis))
    q.getHints.put(QueryHints.TIME_BUCKETS_KEY, 24)
    q.getHints.put(QueryHints.RETURN_ENCODED, java.lang.Boolean.TRUE)
    q
  }

  def getQueryJSON(sft: SimpleFeatureType, query: String): Query = {
    val q = getQuery(sft, query)
    q.getHints.remove(QueryHints.RETURN_ENCODED)
    q
  }

  "TemporalDensityIterator" should {
    val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Geometry:srid=4326"
    val sft = createNewSchema(spec)
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    val encodedFeatures = (0 until 150).toArray.map{
      i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    }
    loadFeatures(sft, encodedFeatures)
    ok

    "reduce total features returned" in {
      val q = getQuery(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")
      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val allFeatures = results.features()
      val iter = allFeatures.toList
      (iter must not).beNull

      iter.length should be lessThan 150
      iter.length mustEqual 1
    }

    "maintain total weights of time" in {
      val q = getQuery(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val iter = results.features().toList
      val sf = iter.head
      sf must not beNull

      val timeSeries = decodeTimeSeries(sf.getAttribute(0).asInstanceOf[String])
      val totalCount = timeSeries.map { case (dateTime, count) => count}.sum

      totalCount mustEqual 150
      timeSeries.size mustEqual 1
    }

    "maintain total weights of time - json" in {
      val q = getQueryJSON(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val iter = results.features().toList
      val sf = iter.head
      sf must not beNull;

      val timeSeries = jsonToTimeSeries(sf.getAttribute(0).asInstanceOf[String])
      val totalCount = timeSeries.map { case (dateTime, count) => count}.sum

      totalCount mustEqual 150
      timeSeries.size mustEqual 1
    }


    "maintain total irrespective of point" in {
      val sft = createNewSchema(spec)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
      }
      loadFeatures(sft, encodedFeatures)

      val q = getQuery(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val sfList = results.features().toList

      val sf = sfList.head
      val timeSeries = decodeTimeSeries(sf.getAttribute(0).asInstanceOf[String])

      val total = timeSeries.map { case (dateTime, count) => count }.sum

      total mustEqual 150
      timeSeries.size mustEqual 1
    }

    "maintain total irrespective of point - json" in {
      val sft = createNewSchema(spec)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, s"POINT(-77.$i 38.$i)")
      }
      loadFeatures(sft, encodedFeatures)

      val q = getQueryJSON(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val sfList = results.features().toList

      val sf = sfList.head
      val timeSeries = jsonToTimeSeries(sf.getAttribute(0).asInstanceOf[String])

      val total = timeSeries.map { case (dateTime, count) => count }.sum

      total mustEqual 150
      timeSeries.size mustEqual 1
    }

    "correctly bin off of time intervals" in {
      val sft = createNewSchema(spec)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      loadFeatures(sft, encodedFeatures)

      val q = getQuery(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val sf = results.features().toList.head
      val timeSeries = decodeTimeSeries(sf.getAttribute(0).asInstanceOf[String])

      val total = timeSeries.map {
        case (dateTime, count) =>
          count mustEqual 2L
          count}.sum

      total mustEqual 48
      timeSeries.size mustEqual 24
    }

    "correctly bin off of time intervals - json" in {
      val sft = createNewSchema(spec)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      loadFeatures(sft, encodedFeatures)

      val q = getQueryJSON(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")


      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val sf = results.features().toList.head
      val timeSeries = jsonToTimeSeries(sf.getAttribute(0).asInstanceOf[String])

      val total = timeSeries.map {
        case (dateTime, count) =>
          count mustEqual 2L
          count}.sum

      total mustEqual 48
      timeSeries.size mustEqual 24
    }

    "encode decode feature" in {
      val timeSeries = new collection.mutable.HashMap[DateTime, Long]()
      timeSeries.put(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC), 2)
      timeSeries.put(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC), 8)

      val encoded = KryoLazyTemporalDensityIterator.encodeTimeSeries(timeSeries)
      val decoded = KryoLazyTemporalDensityIterator.decodeTimeSeries(encoded)

      timeSeries mustEqual decoded
      timeSeries.size mustEqual 2
      timeSeries.get(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC)).get mustEqual 2L
      timeSeries.get(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC)).get mustEqual 8L
    }

    "query dtg bounds not in DataStore" in {
      val sft = createNewSchema(spec)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-02-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      loadFeatures(sft, encodedFeatures)

      val q = getQuery(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val sfList = results.features().toList
      sfList must beEmpty
    }

    "nothing to query over" in {
      val sft = createNewSchema(spec)
      val encodedFeatures = new Array[Array[_]](0)
      loadFeatures(sft, encodedFeatures)

      val q = getQuery(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val sfList = results.features().toList
      sfList must beEmpty
    }

    "nothing to query over - json" in {
      val sft = createNewSchema(spec)
      val encodedFeatures = new Array[Array[_]](0)
      loadFeatures(sft, encodedFeatures)

      val q = getQueryJSON(sft, "(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")

      val results = ds.getFeatureSource(sft.getTypeName).getFeatures(q)
      val sfList = results.features().toList
      sfList must beEmpty
    }
  }
}
