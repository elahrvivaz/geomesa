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

import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import com.vividsolutions.jts.geom._
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
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{GridSnap, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Density iterator - only works on kryo-encoded point geometries
 */
class Z3DensityIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import Z3DensityIterator._

  var sft: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key, Value] = null
  var filter: Filter = null
  var geomIndex: Int = -1

  var gridSnap: GridSnap = null
  var width: Int = -1
  var height: Int = -1

  var result = mutable.Map.empty[(Int, Int), Double]

  var topKey: Key = null
  var topValue: Value = new Value()
  var currentRange: aRange = null

  var handleValue: () => Unit = null
  var weightFn: (SimpleFeature) => Double = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)
//    initClassLoader(logger)

    this.source = src.deepCopy(env)
    val options = jOptions.asScala

    sft = SimpleFeatureTypes.createType("test", options(SFT_OPT))
    filter = options.get(CQL_OPT).map(FastFilterFactory.toFilter).orNull
    geomIndex = sft.getGeomIndex

    val bounds = options(ENVELOPE_OPT).split(",").map(_.toDouble)
    val envelope = new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))
    val xy = options(GRID_OPT).split(",").map(_.toInt)
    width = xy(0)
    height = xy(1)
    gridSnap = new GridSnap(envelope, width, height)

    // function to get the weight from the feature - defaults to 1.0 unless an attribute is specified
    weightFn = options.get(WEIGHT_OPT).map(sft.indexOf).map {
      case i if i == -1 =>
        val expression = ECQL.toExpression(options(WEIGHT_OPT))
        getWeightFromExpression(_: SimpleFeature, expression)
      case i if sft.getDescriptor(i).getType.getBinding == classOf[java.lang.Double] =>
        getWeightFromDouble(_: SimpleFeature, i)
      case i =>
        getWeightFromNonDouble(_: SimpleFeature, i)
    }.getOrElse((sf: SimpleFeature) => 1.0)

    val reusableSf = new KryoFeatureSerializer(sft).getReusableFeature
    handleValue = if (filter == null) {
      () => {
        reusableSf.setBuffer(source.getTopValue.get())
        topKey = source.getTopKey
        writePoint(reusableSf, weightFn(reusableSf))
      }
    } else {
      () => {
        reusableSf.setBuffer(source.getTopValue.get())
        if (filter.evaluate(reusableSf)) {
          topKey = source.getTopKey
          writePoint(reusableSf, weightFn(reusableSf))
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

    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey)) {
      handleValue() // write the record to our aggregated results
      source.next() // Advance the source iterator
    }

    if (result.isEmpty) {
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

  private def getWeightFromDouble(sf: SimpleFeature, i: Int): Double = {
    val d = sf.getAttribute(i).asInstanceOf[java.lang.Double]
    if (d == null) 0.0 else d
  }

  private def getWeightFromNonDouble(sf: SimpleFeature, i: Int): Double = {
    val d = sf.getAttribute(i)
    if (d == null) {
      0.0
    } else {
      try {
        d.toString.toDouble
      } catch {
        case e: NumberFormatException => 0.0
      }
    }
  }

  private def getWeightFromExpression(sf: SimpleFeature, e: Expression): Double = {
    val d = e.evaluate(sf, classOf[java.lang.Double])
    if (d == null) 0.0 else d
  }

  /**
   * Writes a density record from a feature that has a point geometry
   */
  private def writePoint(sf: KryoBufferSimpleFeature, weight: Double): Unit =
    writePointToResult(sf.getAttribute(geomIndex).asInstanceOf[Point], weight)

  protected[iterators] def writePointToResult(pt: Point, weight: Double): Unit =
    writeSnappedPoint((gridSnap.i(pt.getX), gridSnap.j(pt.getY)), weight)

  protected[iterators] def writePointToResult(pt: Coordinate, weight: Double): Unit =
    writeSnappedPoint((gridSnap.i(pt.x), gridSnap.j(pt.y)), weight)

  protected[iterators] def writeSnappedPoint(xy: (Int, Int), weight: Double): Unit =
    result.update(xy, result.getOrElse(xy, 0.0) + weight)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object Z3DensityIterator extends Logging {

  // need to be lazy to avoid class loading issues before init is called
  lazy val DENSITY_SFT = SimpleFeatureTypes.createType("density", "result:String,*geom:Point:srid=4326")
  private lazy val zeroPoint = WKTUtils.read("POINT(0 0)")

  // configuration keys
  private val SFT_OPT        = "sft"
  private val CQL_OPT        = "cql"
  private val ENVELOPE_OPT   = "envelope"
  private val GRID_OPT       = "grid"
  private val WEIGHT_OPT     = "weight"

  /**
   * Creates an iterator config that expects entries to be precomputed bin values
   */
  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                envelope: Envelope,
                gridWidth: Int,
                gridHeight: Int,
                weightAttribute: Option[String],
                priority: Int): IteratorSetting = {
    configure(new IteratorSetting(priority, "z3-density-iter", classOf[Z3DensityIterator]),
      sft, filter, envelope, gridWidth, gridHeight, weightAttribute)
  }

  protected[iterators] def configure(is: IteratorSetting,
                                     sft: SimpleFeatureType,
                                     filter: Option[Filter],
                                     envelope: Envelope,
                                     gridWidth: Int,
                                     gridHeight: Int,
                                     weightAttribute: Option[String]): IteratorSetting = {
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    is.addOption(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.addOption(GRID_OPT, s"$gridWidth,$gridHeight")
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    weightAttribute.foreach(is.addOption(WEIGHT_OPT, _))
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

  def encodeResult(result: mutable.Map[(Int, Int), Double]): Array[Byte] = {
    val output = KryoFeatureSerializer.getOutput()
    result.toList.groupBy(_._1._1).foreach { case (row, cols) =>
      output.writeInt(row, true)
      output.writeInt(cols.size, true)
      cols.foreach { case (xy, weight) =>
        output.writeInt(xy._2, true)
        output.writeDouble(weight)
      }
    }
    output.toBytes
  }

  type GridIterator = (SimpleFeature) => Iterator[(Double, Double, Double)]

  def decodeResult(envelope: Envelope, gridWidth: Int, gridHeight: Int): GridIterator = {
    val gs = new GridSnap(envelope, gridWidth, gridHeight)
    val decode = decodeResult(_: SimpleFeature, gs)
    (f) => decode(f)
  }

  private def decodeResult(sf: SimpleFeature, gridSnap: GridSnap): Iterator[(Double, Double, Double)] =
    decodeResult(sf.getAttribute(0).asInstanceOf[Array[Byte]], gridSnap)

  protected[iterators] def decodeResult(result: Array[Byte],
                                        gridSnap: GridSnap): Iterator[(Double, Double, Double)] = {
    val input = KryoFeatureSerializer.getInput(result)
    new Iterator[(Double, Double, Double)]() {
      private var x = 0.0
      private var colCount = 0
      override def hasNext = input.position < input.limit
      override def next() = {
        if (colCount == 0) {
          x = gridSnap.x(input.readInt(true))
          colCount = input.readInt(true)
        }
        val y = gridSnap.y(input.readInt(true))
        val weight = input.readDouble()
        colCount -= 1
        (x, y, weight)
      }
    }
  }

  var initialized = false
  def initClassLoader(log: Logger) = synchronized {
    if (!initialized) {
      try {
        log.trace("Initializing classLoader")
        // locate the geomesa jars
        this.getClass.getClassLoader match {
          case vfsCl: VFSClassLoader =>
            vfsCl.getFileObjects.map(_.getURL).filter(_.toString.contains("geomesa")).foreach { url =>
              if (log != null) {
                log.debug(s"Found geomesa jar at $url")
              }
              val classLoader = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
              GeoTools.addClassLoader(classLoader)
            }

          case _ => // no -op
        }
      } catch {
        case t: Throwable =>
          if (log != null) {
            log.error("Failed to initialize GeoTools' ClassLoader", t)
          }
      } finally {
        initialized = true
      }
    }
  }
}