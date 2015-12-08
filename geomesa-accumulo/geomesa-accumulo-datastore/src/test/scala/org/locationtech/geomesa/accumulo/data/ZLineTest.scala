/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */
package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.data.{Value, Key}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ZLineTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "name:String,dtg:Date,*geom:LineString:srid=4326"

  addFeatures({
    val sf = new ScalaSimpleFeature("fid1", sft)
    sf.setAttribute("name", "fred")
    sf.setAttribute("dtg", "2015-01-01T12:00:00.000Z")
//    sf.setAttribute("geom", "LINESTRING(-78.5000092574703 38.0272986617359,-78.5000196719491 38.0272519798381," +
//        "-78.5000300864205 38.0272190279085,-78.5000370293904 38.0271853867342,-78.5000439723542 38.027151748305," +
//        "-78.5000509153117 38.027118112621,-78.5000578582629 38.0270844741902,-78.5000648011924 38.0270329867966," +
//        "-78.5000648011781 38.0270165108316,-78.5000682379314 38.026999348366,-78.5000752155953 38.026982185898," +
//        "-78.5000786870602 38.0269657099304,-78.5000856300045 38.0269492339602,-78.5000891014656 38.0269327579921," +
//        "-78.5000960444045 38.0269162820211,-78.5001064588197 38.0269004925451,-78.5001134017528 38.0268847030715," +
//        "-78.50012381616 38.0268689135938,-78.5001307590877 38.0268538106175,-78.5001411734882 38.0268387076367," +
//        "-78.5001550593595 38.0268236046505,-78.5001654737524 38.0268091881659,-78.5001758881429 38.0267954581791," +
//        "-78.5001897740009 38.0267810416871,-78.50059593303 38.0263663951609,-78.5007972751677 38.0261625038609)")
    sf.setAttribute("geom", "LINESTRING(47.28515625 25.576171875, 48 26, 49 27)")
    Seq(sf)
  })

  def printR(e: java.util.Map.Entry[Key, Value]): Unit = {
    val row = Key.toPrintableString(e.getKey.getRow.getBytes, 0, e.getKey.getRow.getLength, e.getKey.getRow.getLength)
    val cf = e.getKey.getColumnFamily.toString
//    val cq = e.getKey.getColumnQualifier.getBytes.map("%02X" format _).mkString
    val value = Key.toPrintableString(e.getValue.get(), 0, e.getValue.getSize, e.getValue.getSize)
    println(s"$row :: $cf :: $value")
  }
  "ZLines" should {
    "add features" in {
      skipped("testing")
      val scanner = ds.connector.createScanner(ds.getTableName(sft.getTypeName, Z3Table), new Authorizations())
//      scanner.foreach(printR)
      println(scanner.toSeq.length)
      scanner.close()
      success
    }
    "return features that are contained" in {
      val filter = "bbox(geom,47,25,50,28) and dtg DURING 2015-01-01T11:00:00.000Z/2015-01-01T13:00:00.000Z"
      val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
      val features = ds.getFeatureSource(sft.getTypeName).getFeatures(query).features().toList
      features must haveLength(1)
      features.head.getID mustEqual "fid1"
    }
    "return features that intersect" in {
      val filter = "bbox(geom,47.5,25,49,26) and dtg DURING 2015-01-01T11:00:00.000Z/2015-01-01T13:00:00.000Z"
      val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
      val features = ds.getFeatureSource(sft.getTypeName).getFeatures(query).features().toList
      features must haveLength(1)
      features.head.getID mustEqual "fid1"
    }
    "not return features that don't intersect" in {
      val filter = "bbox(geom,45,24,46,25) and dtg DURING 2015-01-01T11:00:00.000Z/2015-01-01T13:00:00.000Z"
      val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
      val features = ds.getFeatureSource(sft.getTypeName).getFeatures(query).features().toList
      features must beEmpty
    }
  }
}
