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
import com.google.common.primitives.{Ints, Bytes, Longs}
import com.vividsolutions.jts.geom.Point
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Value, Mutation}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToWrite, FeatureToMutations}
import org.locationtech.geomesa.curve.Z2SFC
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.accumulo.data.EMPTY_TEXT

import scala.util.hashing.MurmurHash3

object Z2Table extends GeoMesaTable {

  val FULL_CF = new Text("F")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val SHARDS = (0 until 10).map(_.toByte).toArray
  val POINT_INDICATOR = 0.toByte

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
        val mutations = getNonPointRowKeys(fw.feature, dtgIndex).map(new Mutation(_))
        mutations.foreach(_.put(FULL_CF, EMPTY_TEXT, fw.columnVisibility, fw.dataValue))
        mutations
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
        val mutations = getNonPointRowKeys(fw.feature, dtgIndex).map(new Mutation(_))
        mutations.foreach(_.putDelete(FULL_CF, EMPTY_TEXT, fw.columnVisibility))
        mutations
      }
    }
  }

  def getRowKeys(sf: SimpleFeature, dtgIndex: Option[Int]): Seq[Text] = {
    sf.getDefaultGeometry match {
      case p: Point => Seq(getPointRowKey(sf, dtgIndex))
      case _        => getNonPointRowKeys(sf, dtgIndex)
    }
  }

  def getPointRowKey(sf: SimpleFeature, dtgIndex: Option[Int]): Text = {
    val tablePrefix = sf.getType.getTableSharingPrefix.getBytes(Charsets.UTF_8)
    val id = sf.getID.getBytes(Charsets.UTF_8)
    val shard = Array(math.abs(MurmurHash3.arrayHash(id) % 10).toByte)
    val pt = sf.getDefaultGeometry.asInstanceOf[Point]
    val ptIndicator = Array(POINT_INDICATOR)
    val z2 = Longs.toByteArray(Z2SFC.index(pt.getX, pt.getY).z)
    val time = encodeTime(dtgIndex.map(sf.getAttribute(_).asInstanceOf[Date].getTime).getOrElse(System.currentTimeMillis))
    val row = Bytes.concat(tablePrefix, shard, z2.take(4), time, z2.drop(4), id)
    new Text(row)
  }

  def getNonPointRowKeys(sf: SimpleFeature, dtgIndex: Option[Int]): Seq[Text] = {

  }

  // encode the time so that it sorts lexically
  def encodeTime(time: Long): Array[Byte] = Longs.toByteArray(time ^ Long.MinValue)

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}
