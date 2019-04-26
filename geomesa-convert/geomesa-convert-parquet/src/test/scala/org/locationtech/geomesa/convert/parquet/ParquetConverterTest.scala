/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import java.io.InputStream

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParquetConverterTest extends Specification {

  sequential

  val sft = SimpleFeatureTypes.createType("test", "fid:Int,name:String,age:Int,lastseen:Date,*geom:Point:srid=4326")

  lazy val file = getClass.getClassLoader.getResource("example.parquet")
  lazy val path = file.toURI.toString

  def bytes(): InputStream = file.openStream()

  "ParquetConverter" should {

    "parse a parquet file" in {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "parquet",
          |   id-field     = "avroPath($0, '/__fid__')",
          |   fields = [
          |     { name = "fid",      transform = "avroPath($0, '/fid')" },
          |     { name = "name",     transform = "avroPath($0, '/name')" },
          |     { name = "age",      transform = "avroPath($0, '/age')" },
          |     { name = "lastseen", transform = "millisToDate(avroPath($0, '/lastseen'))" },
          |     { name = "geom",     transform = "avroGeometry($0, '/geom')" },
          |   ]
          | }
        """.stripMargin)

      val res = WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()
        ec.setInputFilePath(path)
        WithClose(converter.process(bytes(), ec))(_.toList)
      }
      println(res)
      res must haveLength(3)
    }

//    "infer a converter from input data" >> {
//      import scala.collection.JavaConverters._
//
//      val data =
//        """
//          |1,hello,45.0,45.0
//          |2,world,90.0,90.0
//        """.stripMargin
//
//      val factory = new DelimitedTextConverterFactory()
//      val inferred = factory.infer(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)))
//      inferred must beSome
//
//      val (sft, config) = inferred.get
//      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
//          Seq(classOf[Integer], classOf[String], classOf[java.lang.Float], classOf[java.lang.Float], classOf[Point])
//
//      val converter = factory.apply(sft, config)
//      converter must beSome
//
//      val features = converter.get.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))).toList
//      converter.get.close()
//      features must haveLength(2)
//      features(0).getAttributes.asScala mustEqual Seq(1, "hello", 45f, 45f, WKTUtils.read("POINT (45 45)"))
//      features(1).getAttributes.asScala mustEqual Seq(2, "world", 90f, 90f, WKTUtils.read("POINT (90 90)"))
//    }
//
//    "infer a string types from null inputs" >> {
//      import scala.collection.JavaConverters._
//
//      val data =
//        """
//          |num,word,lat,lon
//          |1,,45.0,45.0
//        """.stripMargin
//
//      val factory = new DelimitedTextConverterFactory()
//      val inferred = factory.infer(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)))
//      inferred must beSome
//
//      val (sft, config) = inferred.get
//      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
//          Seq(classOf[Integer], classOf[String], classOf[java.lang.Float], classOf[java.lang.Float], classOf[Point])
//
//      val converter = factory.apply(sft, config)
//      converter must beSome
//
//      val features = converter.get.process(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))).toList
//      converter.get.close()
//      features must haveLength(1)
//      features.head.getAttributes.asScala mustEqual Seq(1, "", 45f, 45f, WKTUtils.read("POINT (45 45)"))
//    }
//
//    "ingest magic files" >> {
//      val data =
//        """id,name:String,age:Int,*geom:Point:srid=4326
//          |fid-0,name0,0,POINT(40 50)
//          |fid-1,name1,1,POINT(41 51)""".stripMargin
//      val is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
//      val features = DelimitedTextConverter.magicParsing("foo", is).toList
//      features must haveLength(2)
//      foreach(0 to 1) { i =>
//        features(i).getID mustEqual s"fid-$i"
//        features(i).getAttributeCount mustEqual 3
//        features(i).getAttribute(0) mustEqual s"name$i"
//        features(i).getAttribute(1) mustEqual i
//        features(i).getAttribute(2) mustEqual WKTUtils.read(s"POINT(4$i 5$i)")
//      }
//    }
  }
}
