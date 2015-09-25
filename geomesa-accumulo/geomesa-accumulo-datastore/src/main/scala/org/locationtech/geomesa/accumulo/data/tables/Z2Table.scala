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
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data.EMPTY_TEXT
import org.locationtech.geomesa.curve.ZRange.ZPrefix
import org.locationtech.geomesa.curve.{Z2, Z2SFC}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3

object Z2Table extends GeoMesaTable {

  val FULL_CF = new Text("f")
  val BIN_CF  = new Text("b")
  val MAP_CF  = new Text("m")

  val SHARDS: Seq[Array[Byte]] = (0 until 10).map(i => Array(i.toByte))
  val POINT_INDICATOR = 0.toByte

  private val NON_POINT_LO: Array[Byte] = Array.fill(4)(0)

  override def supports(sft: SimpleFeatureType): Boolean = sft.getSchemaVersion > 6

  override def suffix: String = "z2"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex
    val getRow: (SimpleFeature) => Text = if (sft.isPoints) {
      (sf) => getPointRowKey(sf, dtgIndex)
    } else {
      (sf) => getNonPointRowKey(sf, dtgIndex)
    }
    (fw: FeatureToWrite) => {
      val mutation = new Mutation(getRow(fw.feature))
      mutation.put(FULL_CF, EMPTY_TEXT, fw.columnVisibility, fw.dataValue)
      mutation.put(MAP_CF, EMPTY_TEXT, fw.columnVisibility, fw.indexValue)
      fw.binValue.foreach(v => mutation.put(BIN_CF, EMPTY_TEXT, fw.columnVisibility, v))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex
    val getRow: (SimpleFeature) => Text = if (sft.isPoints) {
      (sf) => getPointRowKey(sf, dtgIndex)
    } else {
      (sf) => getNonPointRowKey(sf, dtgIndex)
    }
    (fw: FeatureToWrite) => {
      val mutation = new Mutation(getRow(fw.feature))
      mutation.putDelete(FULL_CF, EMPTY_TEXT, fw.columnVisibility)
      mutation.putDelete(MAP_CF, EMPTY_TEXT, fw.columnVisibility)
      mutation.putDelete(BIN_CF, EMPTY_TEXT, fw.columnVisibility)
      Seq(mutation)
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
//val l = math.round(g.getEnvelopeInternal.getWidth)
//val h = math.round(g.getEnvelopeInternal.getHeight)
//
//val decomposed = Z2.decompose(g)
//println(s"Matched ${decomposed.map(_.precision).mkString(",")} bits for envelope $l x $h geometry ${WKTUtils.write(g)}")

        val z2 = Longs.toByteArray(zPrefix).take(4)
        // flip the first 3 bits to indicate a non-point geom
        // these bits will always be 0 (unused) in the z2 value
        // bits flipped indicate the precision of the z value - creates a box
        val z2withPrecision: Array[Byte] = if (bits > 31) {
          // no bits flipped - all 4 bytes are used - essentially the same as a point
          z2
        } else if (bits > 23) {
          // first bit flipped, last byte zeroed
          Array[Byte]((z2.head | 0x80).toByte, z2(1), z2(2), 0)
        } else if (bits > 15) {
          // second bit flipped, last 2 bytes zeroed
          Array[Byte]((z2.head | 0x40).toByte, z2(1), 0, 0)
        } else if (bits > 7) {
          // first two bits flipped, last 2 bytes zeroed
          Array[Byte]((z2.head | 0xc0).toByte, 0, 0, 0)
        } else {
          // third bit flipped, everything else zeroed - this is essentially the whole world
          Array[Byte](0x20.toByte, 0, 0, 0)
        }
        // TODO zboxes don't have much of a common prefix???
        getRowKey(sf, dtgIndex, z2withPrecision, NON_POINT_LO)
    }
  }

  private def getRowKey(sf: SimpleFeature, dtgIndex: Option[Int], zByteHi: Array[Byte], zByteLo: Array[Byte]): Text = {
    val tablePrefix = sf.getType.getTableSharingPrefix.getBytes(Charsets.UTF_8)
    val id = sf.getID.getBytes(Charsets.UTF_8)
    val shard = Array(math.abs(MurmurHash3.arrayHash(id) % 10).toByte)
    val time = encodeTime(dtgIndex.flatMap(i => Option(sf.getAttribute(i).asInstanceOf[Date]).map(_.getTime)))
    val row = Bytes.concat(tablePrefix, shard, zByteHi, time, zByteLo, id)
    new Text(row)
  }

  // encode the time so that it sorts lexically
  def encodeTime(time: Long): Array[Byte] = Longs.toByteArray(time ^ Long.MinValue).take(6)

  def encodeTime(time: Option[Long]): Array[Byte] = encodeTime(time.getOrElse(System.currentTimeMillis()))

  def decodeRow(sft: SimpleFeatureType, row: Array[Byte]): Unit = {
    val prefixLength = sft.getTableSharingPrefix.getBytes(Charsets.UTF_8).length
    val prefix = new String(row.slice(0, prefixLength), Charsets.UTF_8)
    val shard = row(prefixLength)
    val zhi = row.slice(prefixLength + 1, prefixLength + 5)
    val time = Longs.fromByteArray(row.slice(prefixLength + 5, prefixLength + 11) ++ Array(0.toByte, 0.toByte)) ^ Long.MinValue
    val zlo = row.slice(prefixLength + 11, prefixLength + 15)
    val id = new String(row.slice(prefixLength + 15, row.length), Charsets.UTF_8)
    val z = Z2(Longs.fromByteArray(zhi ++ zlo))
    println(s"prefix: $prefix")
    println(s"shard: $shard")
    println(s"time: ${new Date(time)}")
    println(s"z: $z ${Z2SFC.invert(z)}")
    println(s"id: $id")
  }

  def decodeZ(sft: SimpleFeatureType, row: Array[Byte]): (String, String) = {
    val prefixLength = sft.getTableSharingPrefix.getBytes(Charsets.UTF_8).length
    val zhi = row.slice(prefixLength + 1, prefixLength + 5)
    val id = new String(row.slice(prefixLength + 15, row.length), Charsets.UTF_8)
    (id, Hex.encodeHexString(zhi).grouped(2).mkString(":"))
  }

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val localityGroups = Seq(FULL_CF, BIN_CF, MAP_CF).map(t => t.toString -> Set(t).asJava).toMap.asJava
    tableOps.setLocalityGroups(table, localityGroups)
  }
}
