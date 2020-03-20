/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJsonSerializerTest extends Specification {

  "GeoJsonSerializer" should {

    "serialize basic feature collection" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val out = new ByteArrayOutputStream()
      WithClose(GeoJsonSerializer.writer(new OutputStreamWriter(out))) { writer =>
        val serializer = new GeoJsonSerializer(sft)
        serializer.startFeatureCollection(writer)
        serializer.write(writer, sf)
        serializer.endFeatureCollection(writer)
      }

      val serialized = new String(out.toByteArray, StandardCharsets.UTF_8)

println(serialized)
      serialized must not(beEmpty)
//      laz.getID mustEqual sf.getID
//      laz.getAttributes mustEqual sf.getAttributes
//      laz.getAttribute("a") mustEqual sf.getAttribute("a")
//      laz.getAttribute("b") mustEqual sf.getAttribute("b")
//      laz.getAttribute("c") mustEqual sf.getAttribute("c")
//      laz.getAttribute("d") mustEqual sf.getAttribute("d")
//      laz.getAttribute("e") mustEqual sf.getAttribute("e")
//      laz.getAttribute("f") mustEqual sf.getAttribute("f")
//      laz.getAttribute("g") mustEqual sf.getAttribute("g")
//      laz.getAttribute("dtg") mustEqual sf.getAttribute("dtg")
//      laz.getAttribute("geom") mustEqual sf.getAttribute("geom")
    }


    "serialize single feature" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val out = new ByteArrayOutputStream()
      WithClose(GeoJsonSerializer.writer(new OutputStreamWriter(out))) { writer =>
        new GeoJsonSerializer(sft).write(writer, sf)
      }

      val serialized = new String(out.toByteArray, StandardCharsets.UTF_8)

println(serialized)
      serialized must not(beEmpty)
      //      laz.getID mustEqual sf.getID
      //      laz.getAttributes mustEqual sf.getAttributes
      //      laz.getAttribute("a") mustEqual sf.getAttribute("a")
      //      laz.getAttribute("b") mustEqual sf.getAttribute("b")
      //      laz.getAttribute("c") mustEqual sf.getAttribute("c")
      //      laz.getAttribute("d") mustEqual sf.getAttribute("d")
      //      laz.getAttribute("e") mustEqual sf.getAttribute("e")
      //      laz.getAttribute("f") mustEqual sf.getAttribute("f")
      //      laz.getAttribute("g") mustEqual sf.getAttribute("g")
      //      laz.getAttribute("dtg") mustEqual sf.getAttribute("dtg")
      //      laz.getAttribute("geom") mustEqual sf.getAttribute("geom")
    }

    "correctly serialize different geometries" in {
      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
        "f:GeometryCollection,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "LINESTRING(0 2, 2 0, 8 6)")
      sf.setAttribute("b", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
      sf.setAttribute("c", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("d", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
      sf.setAttribute("e", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), " +
        "((-1 5, 2 5, 2 2, -1 2, -1 5)))")
      sf.setAttribute("f", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(55.0 49.0)")

      val out = new ByteArrayOutputStream()
      WithClose(GeoJsonSerializer.writer(new OutputStreamWriter(out))) { writer =>
        new GeoJsonSerializer(sft).write(writer, sf)
      }

      val serialized = new String(out.toByteArray, StandardCharsets.UTF_8)
println(serialized)
      serialized must not(beEmpty)
    }

    "correctly serialize collection types" in {
      val spec = "a:Integer,m:Map[String,Double],l:List[Date],dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("m", Map("test1" -> 1.0, "test2" -> 2.0))
      sf.setAttribute("l", List(new Date(100), new Date(200)))
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val out = new ByteArrayOutputStream()
      WithClose(GeoJsonSerializer.writer(new OutputStreamWriter(out))) { writer =>
        new GeoJsonSerializer(sft).write(writer, sf)
      }

      val serialized = new String(out.toByteArray, StandardCharsets.UTF_8)
println(serialized)
      serialized must not(beEmpty)
    }

    "correctly serialize null values" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
        "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      val out = new ByteArrayOutputStream()
      WithClose(GeoJsonSerializer.writer(new OutputStreamWriter(out))) { writer =>
        new GeoJsonSerializer(sft).write(writer, sf)
      }

      val serialized = new String(out.toByteArray, StandardCharsets.UTF_8)
println(serialized)
      serialized must not(beEmpty)
    }
  }
}
