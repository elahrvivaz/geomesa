/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.knn

import org.geotools.data.Query
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geohash.VincentyModel
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class KNearestNeighborSearchProcessIT extends TestWithFeatureType {

  sequential

  case class TestEntry(wkt: String, id: String) {
    def toFeature: ScalaSimpleFeature = ScalaSimpleFeature.create(sft, id, wkt)
  }

  override val spec = "geom:Point:srid=4326"

  val clusterOfPoints = List[TestEntry](
    TestEntry("POINT( -78.503547 38.035475 )", "rotunda"),
    TestEntry("POINT( -78.503923 38.035536 )", "pavilion I"),
    TestEntry("POINT( -78.504059 38.035308 )", "pavilion III"),
    TestEntry("POINT( -78.504276 38.034971 )", "pavilion V"),
    TestEntry("POINT( -78.504424 38.034628 )", "pavilion VII"),
    TestEntry("POINT( -78.504617 38.034208 )", "pavilion IX"),
    TestEntry("POINT( -78.503833 38.033938 )", "pavilion X"),
    TestEntry("POINT( -78.503601 38.034343 )", "pavilion VIII"),
    TestEntry("POINT( -78.503424 38.034721 )", "pavilion VI"),
    TestEntry("POINT( -78.503180 38.035039 )", "pavilion IV"),
    TestEntry("POINT( -78.503109 38.035278 )", "pavilion II"),
    TestEntry("POINT( -78.505152 38.032704 )", "cabell"),
    TestEntry("POINT( -78.510295 38.034283 )", "beams"),
    TestEntry("POINT( -78.522288 38.032844 )", "mccormick"),
    TestEntry("POINT( -78.520019 38.034511 )", "hep")
  )

  val rng = new Random(0)

  lazy val fs = ds.getFeatureSource(sft.getTypeName)

  step {
    addFeatures(clusterOfPoints.map(_.toFeature))
    addFeatures(generateTestData(1000, 38.149894, -79.073639, 0.30).map(_.toFeature))
  }

  // utility method to generate random points about a central point
  // note that these points will be uniform in cartesian space only
  def generateTestData(num: Int, lat: Double, lon: Double, width: Double): Seq[TestEntry] = {
    Seq.tabulate(num) { i =>
      val id = (100000 + i).toString
      TestEntry(s"POINT(${lon + width * (rng.nextDouble() - 0.5)} ${lat + width * (rng.nextDouble() - 0.5)})", id)
    }
  }

  // generates a single SimpleFeature
  def queryFeature(label: String, lat: Double, lon: Double): SimpleFeature =
    ScalaSimpleFeature.create(sft, label, f"POINT($lon $lat)")

  // generates a very loose query
  def wideQuery: Query = {
    val lat = 38.0
    val lon = -78.50
    val siteSize = 5.0
    val minLat = lat - siteSize
    val maxLat = lat + siteSize
    val minLon = lon - siteSize
    val maxLon = lon + siteSize
    val queryString = s"BBOX(geom,$minLon, $minLat, $maxLon, $maxLat)"
    val ecqlFilter = ECQL.toFilter(queryString)
    //val fs = getTheFeatureSource(tableName, featureName)
    //new Query(featureName, ecqlFilter, transform)
    new Query(sftName, ecqlFilter)
  }

  // begin tests ------------------------------------------------

  "GeoMesaKNearestNeighborSearch" should {
    "find nothing within 10km of a single query point " in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      inputFeatures.add(queryFeature("fan mountain", 37.878219, -78.692649))
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      knn.execute(inputFeatures, dataFeatures, 5, 500.0, 10000.0).size must equalTo(0)
    }

    "find 11 points within 400m of a point when k is set to 15 " in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      inputFeatures.add(queryFeature("madison", 38.036871, -78.502720))
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      knn.execute(inputFeatures, dataFeatures, 15, 50.0, 400.0).size should be equalTo 11
    }

    "handle three query points, one of which will return nothing" in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      inputFeatures.add(queryFeature("madison", 38.036871, -78.502720))
      inputFeatures.add(queryFeature("fan mountain", 37.878219, -78.692649))
      inputFeatures.add(queryFeature("blackfriars", 38.149185, -79.070569))
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      knn.execute(inputFeatures, dataFeatures, 5, 500.0, 5000.0).size must greaterThan(0)
    }

    "handle an empty query point collection" in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      knn.execute(inputFeatures, dataFeatures, 100, 500.0, 5000.0).size must equalTo(0)
    }
    "handle non-point geometries in inputFeatures by ignoring them" in {
      val sft = SimpleFeatureTypes.createType("lineStringKnn", "geom:LineString:srid=4326")
      val inputFeatures = new DefaultFeatureCollection("lineStringKnn", sft)
      val lineSF = SimpleFeatureBuilder.build(sft, List(), "route 29")
      lineSF.setDefaultGeometry(WKTUtils.read(f"LINESTRING(-78.491 38.062, -78.474 38.082)"))
      inputFeatures.add(lineSF)

      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      val res = knn.execute(inputFeatures, dataFeatures, 100, 500.0, 5000.0)
      res.size mustEqual 0
    }
  }

  "runNewKNNQuery" should {
    "return a NearestNeighbors object with features around Charlottesville in correct order" in {
      val orderedFeatureIDs = List("rotunda",
        "pavilion II",
        "pavilion I",
        "pavilion IV",
        "pavilion III",
        "pavilion VI",
        "pavilion V",
        "pavilion VII",
        "pavilion VIII",
        "pavilion IX",
        "pavilion X",
        "cabell",
        "beams",
        "hep",
        "mccormick")
      val knnResults =
        KNNQuery.runNewKNNQuery(fs, wideQuery, 15, 500.0, 2500.0, queryFeature("madison", 38.036871, -78.502720))
      // return the ordered neighbors and extract the SimpleFeatures
      val knnFeatures = knnResults.getK.map { _.sf }
      val knnIDs = knnFeatures.map { _.getID }
      knnIDs must equalTo(orderedFeatureIDs)
    }
    "return a nearestNeighbors object with features around Staunton in correct order" in {
      val k = 10
      val referenceFeature = queryFeature("blackfriars", 38.149185, -79.070569)
      val knnResults =
        KNNQuery.runNewKNNQuery(fs, wideQuery, k, 5000.0, 50000.0, referenceFeature)
      val knnFeatureIDs = knnResults.getK.map { _.sf.getID }
      val directFeatures = SelfClosingIterator(fs.getFeatures().features).toList
      val sortedByDist = directFeatures.sortBy (
        a => VincentyModel.getDistanceBetweenTwoPoints(referenceFeature.point, a.point).getDistanceInMeters).take(k)
      knnFeatureIDs.equals(sortedByDist.map{_.getID}) must beTrue
    }
  }
}
