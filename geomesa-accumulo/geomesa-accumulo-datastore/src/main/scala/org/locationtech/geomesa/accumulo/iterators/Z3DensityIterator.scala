/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry
import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => aRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.accumulo.index.QueryPlanners._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{GridSnap, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Iterator that computes and aggregates 'bin' entries. Currently supports 16 byte entries only.
 */
class Z3DensityIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import Z3DensityIterator._

  var sft: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key, Value] = null
  var filter: Filter = null
  var geomIndex: Int = -1
  var batchSize: Int = -1

  var gridSnap: GridSnap = null
  var result = mutable.Map.empty[Int, mutable.Map[Int, Long]]
  var pointsWritten: Int = -1

  var topKey: Key = null
  var topValue: Value = new Value()
  var currentRange: aRange = null

  var handleValue: () => Unit = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(logger)

    this.source = src.deepCopy(env)
    val options = jOptions.asScala

    sft = SimpleFeatureTypes.createType("test", options(SFT_OPT))
    filter = options.get(CQL_OPT).map(FastFilterFactory.toFilter).orNull
    geomIndex = sft.getGeomIndex
    batchSize = options(BATCH_SIZE_OPT).toInt

    val bounds = options(ENVELOPE_OPT).split(",").map(_.toDouble)
    val envelope = new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))
    val xy = options(GRID_OPT).split(",").map(_.toInt)
    gridSnap = new GridSnap(envelope, xy(0), xy(1))

    // we need to derive the bin values from the features
    val reusableSf = new KryoFeatureSerializer(sft).getReusableFeature
    val geomBinding = sft.getDescriptor(geomIndex).getType.getBinding
    val writeDensity: (KryoBufferSimpleFeature) => Unit =
      if (geomBinding == classOf[Point]) {
        writePoint
      } else if (geomBinding == classOf[MultiPoint]) {
        writeMultiPoint
      } else if (geomBinding == classOf[LineString]) {
        writeLineString
      } else if (geomBinding == classOf[MultiLineString]) {
        writeMultiLineString
      } else if (geomBinding == classOf[Polygon]) {
        writePolygon
      } else if (geomBinding == classOf[MultiPolygon]) {
        writeMultiPolygon
      } else {
        writeGeometry
      }

    handleValue = if (filter == null) {
      () => {
        reusableSf.setBuffer(source.getTopValue.get())
        topKey = source.getTopKey
        writeDensity(reusableSf)
      }
    } else {
      () => {
        reusableSf.setBuffer(source.getTopValue.get())
        if (filter.evaluate(reusableSf)) {
          topKey = source.getTopKey
          writeDensity(reusableSf)
        }
      }
    }
  }

  override def hasTop: Boolean = topKey != null
  override def getTopKey: Key = topKey
  override def getTopValue: Value = topValue

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
    result.clear()
    pointsWritten = 0

    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey) && pointsWritten < batchSize) {
      handleValue() // write the record to our aggregated results
      source.next() // Advance the source iterator
    }

    if (pointsWritten == 0) {
      topKey = null // hasTop will be false
      topValue = null
    } else {
      if (topValue == null) {
        // only re-create topValue if it was nulled out
        topValue = new Value()
      }
      topValue.set(Z3DensityIterator.encodeResult(result))
    }
  }

  /**
   * Writes a density record from a feature that has a point geometry
   */
  private def writePoint(sf: KryoBufferSimpleFeature): Unit =
    writePointToResult(sf.getAttribute(geomIndex).asInstanceOf[Point])


  /**
   * Writes a density record from a feature that has a line string geometry
   */
  private def writeLineString(sf: KryoBufferSimpleFeature): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    var i = 0
    while (i < geom.getNumPoints) {
//      writeBinToBuffer(sf, geom.getPointN(i))
      i += 1
    }
//    handleLineString(result.densityGrid, line.intersection(geoHashGeom).asInstanceOf[LineString])
    // TODO
//    geom.getCoordinates.sliding(2).flatMap { case Array(p0, p1) =>
//      gridSnap.snapLine((p0.getOrdinate(0), p0.getOrdinate(1)), (p1.getOrdinate(0), p1.getOrdinate(1))) // TODO
//    }.toSeq.distinct.foreach(key => result.update(key, result.getOrElse(key, 0L) + 1))
  }

  private def writeMultiLineString(sf: KryoBufferSimpleFeature): Unit = {
//    (0 until multiLineString.getNumGeometries).foreach {
//      i => handleLineString(result.densityGrid, multiLineString.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[LineString])
//    }
  }
  private def writeMultiPoint(sf: KryoBufferSimpleFeature): Unit = {
//    (0 until multiPoint.getNumGeometries).foreach {
//      i => addResultPoint(result.densityGrid, multiPoint.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[Point])
//    }
// TODO
  }

  /** for a given polygon, take the centroid of each polygon from the BBOX coverage grid
    * if the given polygon contains the centroid then it is passed on to addResultPoint */
  def writePolygon(sf: KryoBufferSimpleFeature) = {
    //    val grid = snap.generateCwoverageGrid
    //    val featureIterator = grid.getFeatures.features
    //    featureIterator
    //        .filter{ f => inPolygon.intersects(f.polygon) }
    //        .foreach{ f => addResultPoint(result, f.polygon.getCentroid) }
//    handlePolygon(result.densityGrid, polygon.intersection(geoHashGeom).asInstanceOf[Polygon])
    // TODO
  }

  def writeMultiPolygon(sf: KryoBufferSimpleFeature): Unit = {
//    (0 until multiPolygon.getNumGeometries).foreach {
//      i => handlePolygon(result.densityGrid, multiPolygon.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[Polygon])
//    }
// TODO
  }
  /**
   * Writes a bin record from a feature that has a arbitrary geometry.
   * A single internal point will be written.
   */
  def writeGeometry(sf: KryoBufferSimpleFeature): Unit = {
    // just use centroid
//    addResultPoint(result.densityGrid, someGeometry.getCentroid)
//    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Geometry].getInteriorPoint)
    // TODO
  }

  private def writePointToResult(pt: Point): Unit = writeSnappedPoint(gridSnap.i(pt.getX), gridSnap.j(pt.getY))

  private def writeSnappedPoint(x: Int, y: Int): Unit = {
    val row = result.getOrElseUpdate(x, mutable.Map.empty[Int, Long])
    row.update(y, row.getOrElse(y, 0L) + 1)
    pointsWritten += 1
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object Z3DensityIterator extends Logging {

  // need to be lazy to avoid class loading issues before init is called
  lazy val DENSITY_SFT = SimpleFeatureTypes.createType("density", "weight:Long,*geom:Point:srid=4326")
  lazy val ENCODED_DENSITY_SFT = SimpleFeatureTypes.createType("density", "result:String,*geom:Point:srid=4326")
  private lazy val zeroPoint = WKTUtils.read("POINT(0 0)")

  // configuration keys
  private val SFT_OPT        = "sft"
  private val CQL_OPT        = "cql"
  private val ENVELOPE_OPT   = "envelope"
  private val GRID_OPT       = "grid"
  private val BATCH_SIZE_OPT = "batch"

  /**
   * Creates an iterator config that expects entries to be precomputed bin values
   */
  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                envelope: Envelope,
                gridWith: Int,
                gridHeight: Int,
                batchSize: Int,
                priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "z3-density-iter", classOf[Z3DensityIterator])
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    is.addOption(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.addOption(GRID_OPT, s"$gridWith,$gridHeight")
    is.addOption(BATCH_SIZE_OPT, batchSize.toString)
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    is
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): FeatureFunction = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(1, zeroPoint)
    (e: Entry[Key, Value]) => {
      // set the value directly in the array, as we don't support byte arrays as properties
      // TODO GEOMESA-823 support byte arrays natively
      sf.values(0) = e.getValue.get()
      sf
    }
  }

  def adaptIterator(envelope: Envelope, gridWidth: Int, gridHeight: Int): ExpandFeatures = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    val gridSnap = new GridSnap(envelope, gridWidth, gridHeight)
    val geometryFactory = JTSFactoryFinder.getGeometryFactory
    val decode = decodeResult(_: Array[Byte], sf, gridSnap, geometryFactory)
    (f) => decode(f.getAttribute(0).asInstanceOf[Array[Byte]])
  }

  def encodeResult(result: mutable.Map[Int, mutable.Map[Int, Long]]): Array[Byte] = {
    val output = KryoFeatureSerializer.getOutput()
    result.foreach { case (row, cols) =>
      output.writeInt(row, true)
      output.writeInt(cols.size, true)
      cols.foreach { case (col, count) =>
        output.writeInt(col, true)
        output.writeLong(count, true)
      }
    }
    output.toBytes
  }

  def decodeResult(encoded: Array[Byte],
                   reusableFeature: ScalaSimpleFeature,
                   gridSnap: GridSnap,
                   geometryFactory: GeometryFactory): Iterator[SimpleFeature] = {
    val input = KryoFeatureSerializer.getInput(encoded)
    new Iterator[SimpleFeature]() {
      private var x = 0.0
      private var colCount = 0
      override def hasNext = input.position < input.limit
      override def next() = {
        if (colCount == 0) {
          x = gridSnap.x(input.readInt(true))
          colCount = input.readInt(true)
        }
        val y = gridSnap.y(input.readInt(true))
        val weight = input.readLong(true)
        colCount -= 1
        reusableFeature.setAttribute(0, weight.asInstanceOf[AnyRef])
        reusableFeature.setAttribute(1, geometryFactory.createPoint(new Coordinate(x, y)))
        reusableFeature
      }
    }
  }
}