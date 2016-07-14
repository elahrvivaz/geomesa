/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data

import com.google.common.primitives.Longs
import org.locationtech.geomesa.accumulo.data.tables.Z2Table
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification

class Z2TableTest extends Specification {

  def toString(b: Array[Byte]): String = b.map(_.toInt).mkString(":")

  "Z2Table" should {
    "decompose geometries" in {
      val geom = WKTUtils.read("POLYGON ((42.3 23.9, 41.7 20.0, 48.9 23.0, 42.3 23.9))")
      val zs = Z2Table.zBox(geom).map(z => Longs.toByteArray(z).take(Z2Table.GEOM_Z_NUM_BYTES)).toSeq
      val ranges = Z2SFC.ranges((43.0, 43.0), (23.0, 23.0), 8 * Z2Table.GEOM_Z_NUM_BYTES).map { r =>
        val startBytes = Longs.toByteArray(r.lower).take(Z2Table.GEOM_Z_NUM_BYTES)
        val endBytes = Longs.toByteArray(r.upper).take(Z2Table.GEOM_Z_NUM_BYTES)
        (startBytes, endBytes)
      }
      println
      println("ZS " + zs.length)
      zs.map(toString).sorted.foreach(println)
      println("RANGE")
      println(ranges.headOption.map(r => (toString(r._1), toString(r._2))).get)
      zs.map(toString) must contain(===(toString(ranges.head._1))).atLeastOnce
    }
  }
}
