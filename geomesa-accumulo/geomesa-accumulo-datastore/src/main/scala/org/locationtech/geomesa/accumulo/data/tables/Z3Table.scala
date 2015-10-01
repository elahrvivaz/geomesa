/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.data.tables

import java.util.Date
import java.util.Map.Entry

import com.google.common.base.Charsets
import com.google.common.collect.ImmutableSet
import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Key, Mutation, Range => aRange, Value}
import org.apache.hadoop.io.Text
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data.EMPTY_TEXT
import org.locationtech.geomesa.accumulo.index.QueryPlanners._
import org.locationtech.geomesa.curve.ZRange.ZPrefix
import org.locationtech.geomesa.curve.{Z3, Z3SFC, ZRange}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction}
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.`type`.GeometryDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object Z3Table extends GeoMesaTable {

  val EPOCH = new DateTime(0) // min value we handle - 1970-01-01T00:00:00.000
  val EPOCH_END = EPOCH.plusSeconds(Int.MaxValue) // max value we can calculate - 2038-01-18T22:19:07.000
  val FULL_CF = new Text("F")
  val BIN_CF = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  override def supports(sft: SimpleFeatureType): Boolean =
    sft.getDtgField.isDefined && (sft.getSchemaVersion > 6 || (sft.getSchemaVersion > 4 && sft.isPoints))

  override val suffix: String = "z3"

  // z3 always needs a separate table since we don't include the feature name in the row key
  override def formatTableName(prefix: String, sft: SimpleFeatureType): String =
    GeoMesaTable.formatSoloTableName(prefix, suffix, sft)

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    // TODO add support for date ranges with line strings
    val getRowKeys: (FeatureToWrite, Int) => Seq[Array[Byte]] = if (sft.isPoints) getPointRowKey else getGeomRowKeys
    if (sft.getSchemaVersion > 5) {
      // we know the data is kryo serialized in version 6+
      (fw: FeatureToWrite) => {
        val rows = getRowKeys(fw, dtgIndex)
        // store the duplication factor in the column qualifier for later use
        val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
        rows.map { row =>
          val mutation = new Mutation(row)
          mutation.put(FULL_CF, cq, fw.columnVisibility, fw.dataValue)
          fw.binValue.foreach(v => mutation.put(BIN_CF, cq, fw.columnVisibility, v))
          mutation
        }
      }
    } else {
      // we always want to use kryo - reserialize the value to ensure it
      val writer = new KryoFeatureSerializer(sft)
      (fw: FeatureToWrite) => {
        val rows = getRowKeys(fw, dtgIndex)
        val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
        val payload = new Value(writer.serialize(fw.feature))
        rows.map { row =>
          val mutation = new Mutation(row)
          mutation.put(FULL_CF, cq, fw.columnVisibility, payload)
          fw.binValue.foreach(v => mutation.put(BIN_CF, cq, fw.columnVisibility, v))
          mutation
        }
      }
    }

  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    val getRowKeys: (FeatureToWrite, Int) => Seq[Array[Byte]] = if (sft.isPoints) getPointRowKey else getGeomRowKeys
    (fw: FeatureToWrite) => {
      getRowKeys(fw, dtgIndex).map { row =>
        val mutation = new Mutation(row)
        mutation.putDelete(BIN_CF, EMPTY_TEXT, fw.columnVisibility)
        mutation.putDelete(FULL_CF, EMPTY_TEXT, fw.columnVisibility)
        mutation
      }
    }
  }

  override def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    bd.setRanges(Seq(new aRange()))
    bd.delete()
  }

  def getRowPrefix(x: Double, y: Double, time: Long): Array[Byte] = {
    val (week, z) = getRowZ(x, y, time)
    val prefix = Shorts.toByteArray(week)
    val z3idx = Longs.toByteArray(z)
    Bytes.concat(prefix, z3idx)
  }

  def getRowZ(x: Double, y: Double, time: Long): (Short, Long) = {
    val dtg = new DateTime(time)
    val weeks = epochWeeks(dtg)
    val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
    val z3 = Z3SFC.index(x, y, secondsInWeek)
    (weeks.getWeeks.toShort, z3.z)
  }

  private def getPointRowKey(ftw: FeatureToWrite, dtgIndex: Int): Seq[Array[Byte]] = {
    val geom = ftw.feature.point
    val x = geom.getX
    val y = geom.getY
    val dtg = ftw.feature.getAttribute(dtgIndex).asInstanceOf[Date]
    val time = if (dtg == null) System.currentTimeMillis() else dtg.getTime
    val prefix = getRowPrefix(x, y, time)
    val idBytes = ftw.feature.getID.getBytes(Charsets.UTF_8)
    Seq(Bytes.concat(prefix, idBytes))
  }

  private def getGeomRowKeys(ftw: FeatureToWrite, dtgIndex: Int): Seq[Array[Byte]] = {
    val idBytes = ftw.feature.getID.getBytes(Charsets.UTF_8)
    // TODO allow for date ranges - NOTE we'd have to consider tmin/tmax on a per-week basis
    val dtg = ftw.feature.getAttribute(dtgIndex).asInstanceOf[Date]
    val time = if (dtg == null) System.currentTimeMillis() else dtg.getTime
    val zs = zBox(ftw.feature.getDefaultGeometry.asInstanceOf[Geometry], time).distinct
    zs.map { case (w, z) => Bytes.concat(Shorts.toByteArray(w), Longs.toByteArray(z).take(3), idBytes) }
  }

  private def zBox(geom: Geometry, time: Long): Seq[(Short, Long)] = geom match {
    case g: Point => Seq(getRowZ(g.getX, g.getY, time))
    case g: LineString =>
      (0 until g.getNumPoints).map(g.getPointN).sliding(2).toSeq.flatMap { case Seq(one, two) =>
        val (xmin, xmax) = minMax(one.getX, two.getX)
        val (ymin, ymax) = minMax(one.getY, two.getY)
        zBox(xmin, ymin, xmax, ymax, time)
      }
    case g: GeometryCollection => (0 until g.getNumGeometries).map(g.getGeometryN).flatMap(zBox(_, time))
    case g: Geometry =>
      val env = g.getEnvelopeInternal
      zBox(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY, time)
  }

  private def zBox(xmin: Double, ymin: Double, xmax: Double, ymax: Double, time: Long): Seq[(Short, Long)] = {
    val (wmin, zmin) = getRowZ(xmin, ymin, time)
    val (wmax, zmax) = getRowZ(xmax, ymax, time)
    getZPrefixes(zmin, zmax).map((wmin, _))
  }

  private def minMax(a: Double, b: Double): (Double, Double) = if (a < b) (a, b) else (b, a)

  private def getZPrefixes(zmin: Long, zmax: Long): Seq[Long] = {
    val in = scala.collection.mutable.Queue((zmin, zmax))
    val out = ArrayBuffer.empty[Long]

    while (in.nonEmpty) {
      val (min, max) = in.dequeue()
      val ZPrefix(zprefix, zbits) = ZRange.longestCommonPrefix(min, max, Z3)
      if (zbits > 23) {
        out.append(zprefix & 0xffffff0000000000L) // truncate down to the 3 bytes we use so we can dedupe rows
      } else {
        val (litmax, bigmin) = ZRange.zdivide(Z3((min + max) / 2), Z3(min), Z3(max), Z3)
        in.enqueue((min, litmax.z), (bigmin.z, max))
      }
    }

    out.toSeq
  }

  def adaptZ3KryoIterator(sft: SimpleFeatureType): FeatureFunction = {
    val kryo = new KryoFeatureSerializer(sft)
    (e: Entry[Key, Value]) => {
      // TODO lazy features if we know it's read-only?
      kryo.deserialize(e.getValue.get())
    }
  }


  def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val indexedAttributes = getAttributesToIndex(sft)
    val localityGroups: Map[Text, Text] =
      indexedAttributes.map { case (name, _) => (name, name) }.toMap.+((BIN_CF, BIN_CF)).+((FULL_CF, FULL_CF))
    tableOps.setLocalityGroups(table, localityGroups.map { case (k, v) => (k.toString, ImmutableSet.of(v)) } )
  }

  private def getAttributesToIndex(sft: SimpleFeatureType) =
    sft.getAttributeDescriptors
      .filterNot { d => d.isInstanceOf[GeometryDescriptor] }
      .map { d => (new Text(d.getLocalName.getBytes(Charsets.UTF_8)), sft.indexOf(d.getName)) }
}
