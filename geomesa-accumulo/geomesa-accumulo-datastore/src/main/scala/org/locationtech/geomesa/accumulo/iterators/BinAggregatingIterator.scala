/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Map.Entry
import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import com.vividsolutions.jts.geom.{Geometry, LineString, Point}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => aRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.geotools.factory.GeoTools
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.index.QueryPlanners._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class BinAggregatingIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import BinAggregatingIterator._

  var sft: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key, Value] = null
  var filter: Filter = null
  var geomIndex: Int = -1
  var dtgIndex: Int = -1
  var trackIndex: Int = -1
  var labelIndex: Int = -1
  var batchSize: Int = -1

  var topKey: Key = null
  var topValue: Value = new Value()
  var reusablesf: KryoBufferSimpleFeature = null
  var kryo: KryoFeatureSerializer = null
  var bytes: Array[Byte] = null
  var byteBuffer: ByteBuffer = null
  var binSize: Int = -1
  var currentRange: aRange = null
  var writeBin: () => Unit = null
  var getTrackId: () => Int = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    BinAggregatingIterator.initClassLoader(logger)
    import org.locationtech.geomesa.accumulo.index.getDtgFieldName
    this.source = source.deepCopy(env)
    sft = SimpleFeatureTypes.createType("test", options.get(SFT_OPT))
    filter = Option(options.get(CQL_OPT)).map(FastFilterFactory.toFilter).orNull
    geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    dtgIndex = Option(options.get(DATE_OPT)).map(_.toInt).orElse(getDtgFieldName(sft).map(sft.indexOf))
        .getOrElse(throw new RuntimeException("No dtg"))
    trackIndex = options.get(TRACK_OPT).toInt
    labelIndex = Option(options.get(LABEL_OPT)).map(_.toInt).getOrElse(-1)
    batchSize = options.get(BATCH_OPT).toInt
    kryo = new KryoFeatureSerializer(sft)
    reusablesf = kryo.getReusableFeature
    binSize = if (labelIndex == -1) 16 else 24
    bytes = Array.ofDim(binSize * batchSize)
    byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    getTrackId = if (trackIndex == -1) {
      () => reusablesf.getID.hashCode()
    } else {
      () => reusablesf.getAttribute(trackIndex).hashCode()
    }

    writeBin = if (sft.getGeometryDescriptor.getType.getBinding == classOf[Point]) {
      if (labelIndex == -1) writePoint else writePointWithLabel
    } else if (sft.getGeometryDescriptor.getType.getBinding == classOf[LineString]) {
      if (labelIndex == -1) writePoint else writePointWithLabel
    } else {
      if (labelIndex == -1) writeGeometry else writeGeometryWithLabel
    }
  }

  override def seek(range: aRange, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit = {
    currentRange = range
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def next(): Unit = {
    if (!source.hasTop) {
      topKey = null
      topValue = null
    } else {
      findTop()
    }
  }

  def findTop(): Unit = {
    byteBuffer.clear()
    val maxBytes = batchSize * binSize
    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey) && byteBuffer.position < maxBytes) {
      reusablesf.setBuffer(source.getTopValue.get())
      if (filter == null || filter.evaluate(reusablesf)) {
        topKey = source.getTopKey
        writeBin()
      }
      // Advance the source iterator
      source.next()
    }

    // TODO sort

    if (byteBuffer.position == 0) {
      topKey = null
      topValue = null
    } else if (byteBuffer.position == maxBytes) {
      topValue = new Value(bytes, false)
    } else {
      topValue = new Value(bytes, 0, byteBuffer.position)
    }
  }

  private def writePoint(pt: Point): Unit = {
    byteBuffer.putInt(getTrackId())
    byteBuffer.putInt((reusablesf.getDateAsLong(dtgIndex) / 1000).toInt)
    byteBuffer.putFloat(pt.getY.toFloat) // y is lat // TODO verify lat/lon order
    byteBuffer.putFloat(pt.getX.toFloat) // x is lon
  }

  private def writeLabel(): Unit = {
    byteBuffer.putLong(reusablesf.getAttribute(labelIndex).asInstanceOf[Long])
  }

  def writePoint(): Unit = writePoint(reusablesf.getAttribute(geomIndex).asInstanceOf[Point])

  def writePointWithLabel(): Unit = {
    writePoint(reusablesf.getAttribute(geomIndex).asInstanceOf[Point])
    writeLabel()
  }

  def writeLineString(): Unit = {
    val geom = reusablesf.getAttribute(geomIndex).asInstanceOf[LineString]
    var i = 0
    while (i < geom.getNumPoints) {
      writePoint(geom.getPointN(i))
      i += 1
    }
  }

  def writeLineStringWithLabel(): Unit = {
    val geom = reusablesf.getAttribute(geomIndex).asInstanceOf[LineString]
    var i = 0
    while (i < geom.getNumPoints) {
      writePoint(geom.getPointN(i))
      writeLabel()
      i += 1
    }
  }

  def writeGeometry(): Unit =
    writePoint(reusablesf.getAttribute(geomIndex).asInstanceOf[Geometry].getInteriorPoint)

  def writeGeometryWithLabel(): Unit = {
    writePoint(reusablesf.getAttribute(geomIndex).asInstanceOf[Geometry].getInteriorPoint)
    writeLabel()
  }

  override def hasTop: Boolean = topKey != null
  override def getTopKey: Key = topKey
  override def getTopValue: Value = topValue

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object BinAggregatingIterator {

  // need to be lazy to avoid class loading issues before init is called
  lazy val BIN_SFT = SimpleFeatureTypes.createType("bin", "bin:String,*geom:Point:srid=4326")
  lazy val BIN_ATTRIBUTE_INDEX = BIN_SFT.indexOf("bin")

  val SFT_OPT   = "sft"
  val CQL_OPT   = "cql"
  val TRACK_OPT = "track"
  val LABEL_OPT = "label"
  val DATE_OPT  = "date"
  val BATCH_OPT = "batch"

  private var initialized = false

  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                trackId: String,
                label: Option[String],
                date: Option[String],
                batchSize: Int,
                priority: Int) = {
    val is = new IteratorSetting(priority, "bin-iter", classOf[BinAggregatingIterator])
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    if (trackId == "id") {
      is.addOption(TRACK_OPT, "-1")
    } else {
      is.addOption(TRACK_OPT, sft.indexOf(trackId).toString)
    }

    label.foreach(l => is.addOption(LABEL_OPT, sft.indexOf(l).toString))
    date.foreach(d => is.addOption(DATE_OPT, sft.indexOf(d).toString))
    is.addOption(BATCH_OPT, batchSize.toString)
    is
  }

  def adaptIterator(): FeatureFunction = {
    val sf = new ScalaSimpleFeature("", BIN_SFT)
    sf.setAttribute(1, "POINT(0 0)")
    (e: Entry[Key, Value]) => {
      sf.values(BIN_ATTRIBUTE_INDEX) = e.getValue.get() // TODO support byte arrays natively
      sf
    }
  }

  def initClassLoader(log: Logger) = synchronized {
    if (!initialized) {
      try {
        log.trace("Initializing classLoader")
        // locate the geomesa-distributed-runtime jar
        val cl = this.getClass.getClassLoader
        cl match {
          case vfsCl: VFSClassLoader =>
            var url = vfsCl.getFileObjects.map(_.getURL).filter {
              _.toString.contains("geomesa-distributed-runtime")
            }.head
            if (log != null) log.debug(s"Found geomesa-distributed-runtime at $url")
            var u = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
            GeoTools.addClassLoader(u)

            url = vfsCl.getFileObjects.map(_.getURL).filter {
              _.toString.contains("geomesa-feature")
            }.head
            if (log != null) log.debug(s"Found geomesa-feature at $url")
            u = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
            GeoTools.addClassLoader(u)

          case _ =>
        }
      } catch {
        case t: Throwable =>
          if(log != null) log.error("Failed to initialize GeoTools' ClassLoader ", t)
      } finally {
        initialized = true
      }
    }
  }
}