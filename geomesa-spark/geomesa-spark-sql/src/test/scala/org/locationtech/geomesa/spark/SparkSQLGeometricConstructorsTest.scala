/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.geotools.data.DataStoreFinder
import org.geotools.geometry.jts.JTS
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.locationtech.geomesa.utils.interop.WKTUtils

import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricConstructorsTest extends org.specs2.mutable.Spec with LazyLogging {

  "sql geometry constructors" should {
    sequential

    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")

    val ds = DataStoreFinder.getDataStore(dsParams)
    val spark = SparkSQLTestUtils.createSparkSession()
    val sc = spark.sqlContext

    SparkSQLTestUtils.ingestChicago(ds)

    val df = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()
    logger.debug(df.schema.treeString)
    df.createOrReplaceTempView("chicago")


    "st_box2DFromGeoHash" >> {
      val r = sc.sql(
        s"""
           |select st_box2DFromGeoHash('ezs42', 25)
          """.stripMargin
      )

      val boxCoords = r.collect().head.getAs[Geometry](0).getCoordinates
      val ll = boxCoords(0)
      val ur = boxCoords(2)
      boxCoords.length mustEqual 5
      ll.x must beCloseTo(-5.625, .022) // lon
      ll.y must beCloseTo(42.583, .022) // lat
      ur.x must beCloseTo(-5.581, .022) // lon
      ur.y must beCloseTo(42.627, .022) // lat
    }

    "st_geomFromGeoHash" >> {
      val r = sc.sql(
        s"""
           |select st_geomFromGeoHash('ezs42', 25)
          """.stripMargin
      )

      val geomboxCoords = r.collect().head.getAs[Geometry](0).getCoordinates
      val ll = geomboxCoords(0)
      val ur = geomboxCoords(2)
      geomboxCoords.length mustEqual 5
      ll.x must beCloseTo(-5.625, .022) // lon
      ll.y must beCloseTo(42.583, .022) // lat
      ur.x must beCloseTo(-5.581, .022) // lon
      ur.y must beCloseTo(42.627, .022) // lat
    }

    "st_geomFromWKT" >> {
      val r = sc.sql(
        """
          |select st_geomFromWKT('POINT(0 0)')
        """.stripMargin
      )

      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_geometryFromText" >> {
      val r = sc.sql(
        """
          |select st_geometryFromText('POINT(0 0)')
        """.stripMargin
      )

      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_geomFromWKB" >> {
      val geomArr = Array[Byte](0,
        0, 0, 0, 3,
        0, 0, 0, 1,
        0, 0, 0, 5,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
      )
      val r = sc.sql(
        s"""select st_geomFromWKB(st_byteArray('${new String(geomArr)}'))"""
      )
      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")
    }

    "st_lineFromText" >> {
      val r = sc.sql(
        """
          |select st_lineFromText('LINESTRING(0 0, 1 1, 2 2)')
        """.stripMargin
      )
      r.collect().head.getAs[LineString](0) mustEqual WKTUtils.read("LINESTRING(0 0, 1 1, 2 2)")
    }

    "st_makeBBOX" >> {
      val r = sc.sql(
        """
          |select st_makeBBOX(0.0, 0.0, 2.0, 2.0)
        """.stripMargin
      )
      r.collect().head.getAs[Geometry](0) mustEqual JTS.toGeometry(BoundingBox(0, 2, 0, 2))
    }

    "st_makeBox2D" >> {
      val r = sc.sql(
        """
          |select st_makeBox2D(st_castToPoint(st_geomFromWKT('POINT(0 0)')),
          |                    st_castToPoint(st_geomFromWKT('POINT(2 2)')))
        """.stripMargin
      )
      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    "st_makePolygon" >> {
      val r = sc.sql(
        s"""
           |select st_makePolygon(st_castToLineString(
           |    st_geomFromWKT('LINESTRING(0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0))")
    }

    "st_makePoint" >> {
      val r = sc.sql(
        """
          |select st_makePoint(0, 0)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_makePointM" >> {
      val r = sc.sql(
        """
          |select st_makePointM(0, 0, 1)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0 1)")
    }

    "st_mLineFromText" >> {
      val r = sc.sql(
        """
          |select st_mLineFromText('MULTILINESTRING((0 0, 1 1, 2 2), (0 1, 1 2, 2 3))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiLineString](0) mustEqual WKTUtils.read("MULTILINESTRING((0 0, 1 1, 2 2), " +
        "(0 1, 1 2, 2 3))")
    }

    "st_mPointFromText" >> {
      val r = sc.sql(
        """
          |select st_mPointFromText('MULTIPOINT((0 0), (1 1))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiPoint](0) mustEqual WKTUtils.read("MULTIPOINT((0 0), (1 1))")
    }

    "st_mPolyFromText" >> {
      val r = sc.sql(
        """
          |select st_mPolyFromText('MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 )),((-4 4, 4 4, 4 -4, -4 -4, -4 4),
          |                                    (2 2, -2 2, -2 -2, 2 -2, 2 2)))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiPolygon](0) mustEqual
        WKTUtils.read("MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 ))," +
          "((-4 4, 4 4, 4 -4, -4 -4, -4 4),(2 2, -2 2, -2 -2, 2 -2, 2 2)))")
    }

    "st_point" >> {
      val r = sc.sql(
        """
          |select st_point(0, 0)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_pointFromGeoHash" >> {
      val r = sc.sql(
        s"""
           |select st_pointFromGeoHash('ezs42', 25)
        """.stripMargin
      )

      val point = r.collect().head.getAs[Point](0)
      point.getX must beCloseTo(-5.603, .022)
      point.getY must beCloseTo(42.605, .022)
    }

    "st_pointFromText" >> {
      val r = sc.sql(
        """
          |select st_pointFromText('Point(0 0)')
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_pointFromWKB" >> {
      val pointArr = Array[Byte](0, 0, 0, 0, 1,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0)
      val r = sc.sql(
        s"""
           |select st_pointFromWKB(st_byteArray('${new String(pointArr)}'))
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_polygon" >> {
      val r = sc.sql(
        s"""
           |select st_polygon(st_castToLineString(
           |    st_geomFromWKT('LINESTRING(0 0, 2 2, 5 2, 3 0, 0 0)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0 0, 2 2, 5 2, 3 0, 0 0))")
    }

    "st_polygonFromText" >> {
      val r = sc.sql(
        """
          |select st_polygonFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    // after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
