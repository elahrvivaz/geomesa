/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.tables

import java.util.Date

import com.google.common.base.Charsets
import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data.EMPTY_TEXT
import org.locationtech.geomesa.curve.ZRange.ZPrefix
import org.locationtech.geomesa.curve.{Z2, Z2SFC}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.hashing.MurmurHash3

object Z2Table extends GeoMesaTable {

  val FULL_CF = new Text("F")
  val SHARDS: Array[Byte] = (0 until 10).map(_.toByte).toArray
  val POINT_INDICATOR = 0.toByte
  private val NON_POINT_LO: Array[Byte] = Array.fill(4)(0)

  override def supports(sft: SimpleFeatureType): Boolean = sft.getSchemaVersion > 6

  override def suffix: String = "z2"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex
    if (sft.getGeometryDescriptor.getType.getBinding == classOf[Point]) {
      (fw: FeatureToWrite) => {
        val mutation = new Mutation(getPointRowKey(fw.feature, dtgIndex))
        mutation.put(FULL_CF, EMPTY_TEXT, fw.columnVisibility, fw.dataValue)
        Seq(mutation)
      }
    } else {
      (fw: FeatureToWrite) => {
        val mutation = new Mutation(getNonPointRowKey(fw.feature, dtgIndex))
        mutation.put(FULL_CF, EMPTY_TEXT, fw.columnVisibility, fw.dataValue)
        Seq(mutation)
      }
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex
    if (sft.getGeometryDescriptor.getType.getBinding == classOf[Point]) {
      (fw: FeatureToWrite) => {
        val mutation = new Mutation(getPointRowKey(fw.feature, dtgIndex))
        mutation.putDelete(FULL_CF, EMPTY_TEXT, fw.columnVisibility)
        Seq(mutation)
      }
    } else {
      (fw: FeatureToWrite) => {
        val mutation = new Mutation(getNonPointRowKey(fw.feature, dtgIndex))
        mutation.putDelete(FULL_CF, EMPTY_TEXT, fw.columnVisibility)
        Seq(mutation)
      }
    }
  }

  def getPointRowKey(sf: SimpleFeature, dtgIndex: Option[Int]): Text = {
    val pt = sf.getDefaultGeometry.asInstanceOf[Point]
    val z2 = Longs.toByteArray(Z2SFC.index(pt.getX, pt.getY).z)
    getRowKey(sf, dtgIndex, z2.take(4), z2.drop(4))
  }

  def getNonPointRowKey(sf: SimpleFeature, dtgIndex: Option[Int]): Text = {
    sf.getDefaultGeometry match {
      case p: Point => getPointRowKey(sf, dtgIndex)
      case g: Geometry =>
        val ZPrefix(zPrefix, bits) = Z2.zBox(g)
        val z2 = Longs.toByteArray(zPrefix).take(4)
        // flip the first 3 bits to indicate a non-point geom
        // these bits will always be 0 (unused) in the z2 value
        // bits flipped indicate the precision of the z value - creates a box
        val z2withPrecision: Array[Byte] = if (bits > 32) {
          // no bits flipped - all 4 bytes are used
          z2
        } else if (bits > 24) {
          // first bit flipped, last byte zeroed
          Array((z2.head | 0x80).toByte) ++ z2.tail.take(2) ++ Array[Byte](0)
        } else if (bits > 16) {
          // first two bits flipped, last 2 bytes zeroed
          Array((z2.head | 0xc0).toByte) ++ z2.tail.take(1) ++ Array[Byte](0, 0)
        } else if (bits > 8) {
          // first, 3rd bit flipped, last 3 bytes zeroed
          Array((z2.head | 0xa0).toByte) ++ Array[Byte](0, 0, 0)
        } else {
          // first three bits flipped, everything else zeroed
          Array[Byte](0xe0.toByte, 0, 0, 0)
        }
        getRowKey(sf, dtgIndex, z2withPrecision, NON_POINT_LO)
    }
  }

  private def getRowKey(sf: SimpleFeature, dtgIndex: Option[Int], zByteHi: Array[Byte], zByteLo: Array[Byte]): Text = {
    val tablePrefix = sf.getType.getTableSharingPrefix.getBytes(Charsets.UTF_8)
    val id = sf.getID.getBytes(Charsets.UTF_8)
    val shard = Array(math.abs(MurmurHash3.arrayHash(id) % 10).toByte)
    val pt = sf.getDefaultGeometry.asInstanceOf[Point]
    val time = encodeTime(dtgIndex.map(sf.getAttribute(_).asInstanceOf[Date].getTime))
    val row = Bytes.concat(tablePrefix, shard, zByteHi, time, zByteLo, id)
    new Text(row)
  }

  // encode the time so that it sorts lexically
  def encodeTime(time: Long): Array[Byte] = Longs.toByteArray(time ^ Long.MinValue).take(6)

  def encodeTime(time: Option[Long]): Array[Byte] = encodeTime(time.getOrElse(System.currentTimeMillis()))

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}
