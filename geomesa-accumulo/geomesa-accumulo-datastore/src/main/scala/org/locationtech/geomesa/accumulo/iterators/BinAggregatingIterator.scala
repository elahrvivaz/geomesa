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
import java.util.{Collection => jCollection, Date, Map => jMap}

import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => aRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.geotools.data.Query
import org.geotools.factory.GeoTools
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.index
import org.locationtech.geomesa.accumulo.index.QueryPlanners._
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{SimpleFeatureDeserializers, SimpleFeatureDeserializer, ScalaSimpleFeature}
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

class BinAggregatingIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import BinAggregatingIterator._

  var sft: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key, Value] = null
  var filter: Filter = null
  var geomIndex: Int = -1
  var dtgIndex: Int = -1
  var trackIndex: Int = -1
  var sort: Boolean = false

  var topKey: Key = null
  var topValue: Value = new Value()
  var currentRange: aRange = null

  var bytes: Array[Byte] = null
  var byteBuffer: ByteBuffer = null
  var bytesWritten: Int = -1
  var batchSize: Int = -1

  var handleValue: () => Unit = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    BinAggregatingIterator.initClassLoader(logger)

    source = src.deepCopy(env)
    val options = jOptions.asScala

    sft = SimpleFeatureTypes.createType("test", options(SFT_OPT))
    filter = options.get(CQL_OPT).map(FastFilterFactory.toFilter).orNull

    batchSize = options(BATCH_SIZE_OPT).toInt * BIN_SIZE
    bytes = Array.ofDim(batchSize)
    byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    geomIndex = sft.getGeomIndex
    dtgIndex = options(DATE_OPT).toInt
    trackIndex = options(TRACK_OPT).toInt
    sort = options(SORT_OPT).toBoolean

    if (options.get(BIN_CF_OPT).exists(_.toBoolean)) {
      // we are using the pre-computed bin values
      handleValue = if (filter == null) {
        () => {
          topKey = source.getTopKey
          System.arraycopy(source.getTopValue.get, 0, bytes, bytesWritten, BIN_SIZE)
          bytesWritten += BIN_SIZE
        }
      } else {
        val sf = new ScalaSimpleFeature("", sft)
        val gf = new GeometryFactory
        () => {
          setValuesFromBin(sf, gf)
          if (filter.evaluate(sf)) {
            topKey = source.getTopKey
            System.arraycopy(source.getTopValue.get, 0, bytes, bytesWritten, BIN_SIZE)
            bytesWritten += BIN_SIZE
          }
        }
      }
    } else {
      // we need to derive the bin values from the features
      val reusablesf = new KryoFeatureSerializer(sft).getReusableFeature
      val writeBin = if (sft.getGeometryDescriptor.getType.getBinding == classOf[Point]) {
        () => writePoint(reusablesf)
      } else if (sft.getGeometryDescriptor.getType.getBinding == classOf[LineString]) {
        () => writeLineString(reusablesf)
      } else {
        () => writeGeometry(reusablesf)
      }
      handleValue = if (filter == null) {
        () => {
          reusablesf.setBuffer(source.getTopValue.get())
          topKey = source.getTopKey
          writeBin()
        }
      } else {
        () => {
          reusablesf.setBuffer(source.getTopValue.get())
          if (filter.evaluate(reusablesf)) {
            topKey = source.getTopKey
            writeBin()
          }
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
    byteBuffer.clear()
    bytesWritten = 0

    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey) && bytesWritten < batchSize) {
      handleValue()
      // Advance the source iterator
      source.next()
    }

    if (bytesWritten == 0) {
      topKey = null
      topValue = null
    } else {
      if (topValue == null) {
        topValue = new Value()
      }
      if (sort) {
        BinSorter.quickSort(bytes, 0, bytesWritten - BIN_SIZE)
      }
      if (bytesWritten == batchSize) {
        // use the existing buffer if possible
        topValue.set(bytes)
      } else {
        // if not, we have to copy it
        val copy = Array.ofDim[Byte](bytesWritten)
        System.arraycopy(bytes, 0, copy, 0, bytesWritten)
        topValue.set(copy)
      }
    }
  }

  private def writePoint(sf: KryoBufferSimpleFeature, pt: Point): Unit = {
    byteBuffer.putInt(sf.getAttribute(trackIndex).hashCode())
    byteBuffer.putInt((sf.getDateAsLong(dtgIndex) / 1000).toInt)
    byteBuffer.putFloat(pt.getY.toFloat) // y is lat
    byteBuffer.putFloat(pt.getX.toFloat) // x is lon
    bytesWritten += BIN_SIZE
  }

  def writePoint(sf: KryoBufferSimpleFeature): Unit =
    writePoint(sf, sf.getAttribute(geomIndex).asInstanceOf[Point])

  def writeLineString(sf: KryoBufferSimpleFeature): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    var i = 0
    while (i < geom.getNumPoints) {
      writePoint(sf, geom.getPointN(i))
      i += 1
    }
  }

  def writeGeometry(sf: KryoBufferSimpleFeature): Unit =
    writePoint(sf, sf.getAttribute(geomIndex).asInstanceOf[Geometry].getInteriorPoint)

  def setValuesFromBin(sf: ScalaSimpleFeature, gf: GeometryFactory): Unit = {
    val values = Convert2ViewerFunction.decode(source.getTopValue.get)
    sf.setAttribute(geomIndex, gf.createPoint(new Coordinate(values.lat, values.lon)))
    sf.setAttribute(trackIndex, values.trackId.orNull)
    sf.setAttribute(dtgIndex, new Date(values.dtg))
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object BinAggregatingIterator extends Logging {

  private var initialized = false

  // need to be lazy to avoid class loading issues before init is called
  lazy val BIN_SFT = SimpleFeatureTypes.createType("bin", "bin:String,*geom:Point:srid=4326")
  val BIN_ATTRIBUTE_INDEX = 0 // index of 'bin' attribute in BIN_SFT
  private lazy val zeroPoint = WKTUtils.read("POINT(0 0)")

  val BIN_SIZE = 16
  val BATCH_SIZE_SYS_PROP = "geomesa.output.bin.batch.size"

  val SFT_OPT = "sft"
  val CQL_OPT = "cql"
  val BATCH_SIZE_OPT = "batch"

  val BIN_CF_OPT = "bincf"
  val TRACK_OPT = "track"
  val DATE_OPT = "date"
  val SORT_OPT = "sort"

  def configurePrecomputed(sft: SimpleFeatureType,
                           filter: Option[Filter],
                           sort: Boolean,
                           priority: Int): IteratorSetting = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val config = for (trackId <- sft.getBinTrackId; dtg <- sft.getDtgField) yield {
      val is = configureDynamic(sft, filter, trackId, dtg, sort, priority)
      is.addOption(BIN_CF_OPT, "true")
      is
    }
    config.getOrElse(throw new RuntimeException(s"No default trackId or dtg field found in SFT $sft"))
  }

  def configureDynamic(sft: SimpleFeatureType,
                       filter: Option[Filter],
                       trackId: String,
                       dtg: String,
                       sort: Boolean,
                       priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "bin-iter", classOf[BinAggregatingIterator])
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    is.addOption(BATCH_SIZE_OPT, getBatchSize.toString)
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    if (trackId == "id") {
      is.addOption(TRACK_OPT, "-1")
    } else {
      is.addOption(TRACK_OPT, sft.indexOf(trackId).toString)
    }
    is.addOption(DATE_OPT, sft.indexOf(dtg).toString)
    is.addOption(SORT_OPT, sort.toString)
    is
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   *
   * @return
   */
  def adaptIterator(): FeatureFunction = {
    val sf = new ScalaSimpleFeature("", BIN_SFT)
    sf.setAttribute(1, zeroPoint)
    (e: Entry[Key, Value]) => {
      // set the value directly in the array, as we don't support byte arrays as properties
      // TODO support byte arrays natively
      sf.values(BIN_ATTRIBUTE_INDEX) = e.getValue.get()
      sf
    }
  }

  /**
   * Fallback for when we can't use the aggregating iterator
   * (for example, if the features are avro encode)
   * instead do bin conversion in client.
   *
   * Only supports 1 bin per geom.
   */
  def adaptNonAggregatedIterator(query: Query,
                                 sft: SimpleFeatureType,
                                 serializationType: SerializationType): FeatureFunction = {
    import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
    val sf = new ScalaSimpleFeature("", BIN_SFT)
    sf.setAttribute(1, zeroPoint)
    val returnSft = index.getTransformSchema(query).getOrElse(sft)
    val deserializer = SimpleFeatureDeserializers(returnSft, serializationType)
    val trackIdIndex = returnSft.indexOf(query.getHints.getBinTrackId)
    val dtgIndex = sft.getDtgIndex.get

    (e: Entry[Key, Value]) => {
      val deserialized = deserializer.deserialize(e.getValue.get())
      val dtg = deserialized.getAttribute(dtgIndex).asInstanceOf[Date].getTime
      val trackId = Option(deserialized.getAttribute(trackIdIndex)).map(_.toString)
      val (lat, lon) = {
        val geom = deserialized.getDefaultGeometry.asInstanceOf[Geometry].getInteriorPoint
        (geom.getY.toFloat, geom.getX.toFloat) // TODO ensure order doesn't ever get flipped
      }
      val values = BasicValues(lat, lon, dtg, trackId)
      // set the value directly in the array, as we don't support byte arrays as properties
      // TODO support byte arrays natively
      sf.values(BIN_ATTRIBUTE_INDEX) = Convert2ViewerFunction.encodeToByteArray(values)
      sf
    }
  }

  // normally this should be called from the client machine, as this is where the sys prop would be set
  def getBatchSize: Int =
    Option(System.getProperty(BATCH_SIZE_SYS_PROP)).map(_.toInt).getOrElse(65536) // 1MB for 16 byte bins

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
          if (log != null) log.error("Failed to initialize GeoTools' ClassLoader ", t)
      } finally {
        initialized = true
      }
    }
  }
}

object BinSorter extends Logging {

  import BinAggregatingIterator.BIN_SIZE

  /**
   * If the length of an array to be sorted is less than this
   * constant, insertion sort is used in preference to Quicksort.
   *
   * This length is 'logical' length, so the array is really BIN_SIZE * length
   */
  private val INSERTION_SORT_THRESHOLD = 3

  private val swapBuffers = new ThreadLocal[Array[Byte]]() {
    override def initialValue() = Array.ofDim[Byte](BIN_SIZE)
  }

  private val priorityOrdering = new Ordering[(Array[Byte], Int)]() {
    override def compare(x: (Array[Byte], Int), y: (Array[Byte], Int)) =
      BinSorter.compare(y._1, y._2, x._1, x._2) // reverse for priority queue
  }

  /**
   * Compares two bin chunks by date
   */
  def compare(left: Array[Byte], leftOffset: Int, right: Array[Byte], rightOffset: Int): Int =
    compareIntLittleEndian(left, leftOffset + 4, right, rightOffset + 4) // offset + 4 is dtg

  /**
   * Comparison based on the integer encoding used by ByteBuffer
   * original code is in private/protected java.nio packages
   */
  private def compareIntLittleEndian(left: Array[Byte],
                                     leftOffset: Int,
                                     right: Array[Byte],
                                     rightOffset: Int): Int = {
    val l3 = left(leftOffset + 3)
    val r3 = right(rightOffset + 3)
    if (l3 < r3) {
      return -1
    } else if (l3 > r3) {
      return 1
    }
    val l2 = left(leftOffset + 2) & 0xff
    val r2 = right(rightOffset + 2) & 0xff
    if (l2 < r2) {
      return -1
    } else if (l2 > r2) {
      return 1
    }
    val l1 = left(leftOffset + 1) & 0xff
    val r1 = right(rightOffset + 1) & 0xff
    if (l1 < r1) {
      return -1
    } else if (l1 > r1) {
      return 1
    }
    val l0 = left(leftOffset) & 0xff
    val r0 = right(rightOffset) & 0xff
    if (l0 == r0) {
      0
    } else if (l0 < r0) {
      -1
    } else {
      1
    }
  }

  /**
   * Takes a sequence of (already sorted) aggregates and combines them in a final sort. Uses
   * a priority queue to compare the head element across each aggregate.
   */
  def mergeSort(aggregates: Iterator[Array[Byte]]): Iterator[(Array[Byte], Int)] = {
    if (aggregates.isEmpty) {
      return Iterator.empty
    }
    val queue = new scala.collection.mutable.PriorityQueue[(Array[Byte], Int)]()(priorityOrdering)
    val sizes = scala.collection.mutable.ArrayBuffer.empty[Int]
    while (aggregates.hasNext) {
      val next = aggregates.next()
      sizes.append(next.length / BIN_SIZE)
      queue.enqueue((next, 0))
    }

    logger.debug(s"Got back ${queue.length} aggregates with an average size of ${sizes.sum / sizes.length}" +
        s" chunks and a median size of ${sizes.sorted.apply(sizes.length / 2)} chunks")

    new Iterator[(Array[Byte], Int)] {
      override def hasNext = queue.nonEmpty
      override def next() = {
        val (aggregate, offset) = queue.dequeue()
        if (offset < aggregate.length - BIN_SIZE) {
          queue.enqueue((aggregate, offset + BIN_SIZE))
        }
        (aggregate, offset)
      }
    }
  }

  /**
   * Performs a merge sort into a new byte array
   */
  def mergeSort(left: Array[Byte], right: Array[Byte]): Array[Byte] = {
    if (left.length == 0) {
      return right
    } else if (right.length == 0) {
      return left
    }
    val result = Array.ofDim[Byte](left.length + right.length)
    var (leftIndex, rightIndex, resultIndex) = (0, 0, 0)

    while (leftIndex < left.length && rightIndex < right.length) {
      if (compare(left, leftIndex, right, rightIndex) > 0) {
        System.arraycopy(right, rightIndex, result, resultIndex, BIN_SIZE)
        rightIndex += BIN_SIZE
      } else {
        System.arraycopy(left, leftIndex, result, resultIndex, BIN_SIZE)
        leftIndex += BIN_SIZE
      }
      resultIndex += BIN_SIZE
    }
    while (leftIndex < left.length) {
      System.arraycopy(left, leftIndex, result, resultIndex, BIN_SIZE)
      leftIndex += BIN_SIZE
      resultIndex += BIN_SIZE
    }
    while (rightIndex < right.length) {
      System.arraycopy(right, rightIndex, result, resultIndex, BIN_SIZE)
      rightIndex += BIN_SIZE
      resultIndex += BIN_SIZE
    }
    result
  }

  /**
   * Sorts the specified range of the array by Dual-Pivot Quicksort.
   * Modified version of java's DualPivotQuicksort
   *
   * @param bytes the array to be sorted
   * @param left the index of the first element, inclusive, to be sorted
   * @param right the index of the last element, inclusive, to be sorted
   */
  def quickSort(bytes: Array[Byte], left: Int, right: Int): Unit =
    quickSort(bytes, left, right, leftmost = true)

  /**
   * Optimized for non-leftmost insertion sort
   */
  private def quickSort(bytes: Array[Byte], left: Int, right: Int, leftmost: Boolean): Unit = {

    val length = (right + BIN_SIZE - left) / BIN_SIZE

    if (length < INSERTION_SORT_THRESHOLD) {
      // Use insertion sort on tiny arrays
      if (leftmost) {
        // Traditional (without sentinel) insertion sort is used in case of the leftmost part
        var i = left + BIN_SIZE
        while (i <= right) {
          var j = i
          val ai = getThreadLocalChunk(bytes, i)
          while (j > left && compare(bytes, j - BIN_SIZE, ai, 0) > 0) {
            System.arraycopy(bytes, j - BIN_SIZE, bytes, j, BIN_SIZE)
            j -= BIN_SIZE
          }
          if (j != i) {
            // we don't need to copy if nothing moved
            System.arraycopy(ai, 0, bytes, j, BIN_SIZE)
          }
          i += BIN_SIZE
        }
      } else {
        // optimized insertions sort when we know we have 'sentinel' elements to the left
        /*
         * Every element from adjoining part plays the role
         * of sentinel, therefore this allows us to avoid the
         * left range check on each iteration. Moreover, we use
         * the more optimized algorithm, so called pair insertion
         * sort, which is faster (in the context of Quicksort)
         * than traditional implementation of insertion sort.
         */
        // Skip the longest ascending sequence
        var i = left
        do {
          if (i >= right) {
            return
          }
        } while ({ i += BIN_SIZE; compare(bytes, i , bytes, i - BIN_SIZE) >= 0 })

        var k = i
        val a1 = Array.ofDim[Byte](BIN_SIZE)
        val a2 = Array.ofDim[Byte](BIN_SIZE)
        while ({ i += BIN_SIZE; i } <= right) {
          if (compare(bytes, k, bytes, i) < 0) {
            System.arraycopy(bytes, k, a2, 0, BIN_SIZE)
            System.arraycopy(bytes, i, a1, 0, BIN_SIZE)
          } else {
            System.arraycopy(bytes, k, a1, 0, BIN_SIZE)
            System.arraycopy(bytes, i, a2, 0, BIN_SIZE)
          }
          while ({ k -= BIN_SIZE; compare(a1, 0, bytes, k) < 0 }) {
            System.arraycopy(bytes, k, bytes, k + 2 * BIN_SIZE, BIN_SIZE)
          }
          k += BIN_SIZE
          System.arraycopy(a1, 0, bytes, k + BIN_SIZE, BIN_SIZE)
          while ({ k -= BIN_SIZE; compare(a2, 0, bytes, k) < 0 }) {
            System.arraycopy(bytes, k, bytes, k + BIN_SIZE, BIN_SIZE)
          }
          System.arraycopy(a2, 0, bytes, k + BIN_SIZE, BIN_SIZE)

          i += BIN_SIZE
          k = i
        }

        var j = right
        val last = getThreadLocalChunk(bytes, j)
        while ({ j -= BIN_SIZE; compare(last, 0, bytes, j) < 0 }) {
          System.arraycopy(bytes, j, bytes, j + BIN_SIZE, BIN_SIZE)
        }
        System.arraycopy(last, 0, bytes, j + BIN_SIZE, BIN_SIZE)
      }
      return
    }

    /*
     * Sort five evenly spaced elements around (and including) the
     * center element in the range. These elements will be used for
     * pivot selection as described below. The choice for spacing
     * these elements was empirically determined to work well on
     * a wide variety of inputs.
     */
    val seventh = (length / 7) * BIN_SIZE

    val e3 = (((left + right) / BIN_SIZE) / 2) * BIN_SIZE // The midpoint
    val e2 = e3 - seventh
    val e1 = e2 - seventh
    val e4 = e3 + seventh
    val e5 = e4 + seventh

    def swap(left: Int, right: Int) = {
      val chunk = getThreadLocalChunk(bytes, left)
      System.arraycopy(bytes, right, bytes, left, BIN_SIZE)
      System.arraycopy(chunk, 0, bytes, right, BIN_SIZE)
    }

    // Sort these elements using insertion sort
    if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }

    if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
      if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }
    }
    if (compare(bytes, e4, bytes, e3) < 0) { swap(e4, e3)
      if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
        if (compare(bytes, e2, bytes, e1) < 0) {swap(e2, e1) }
      }
    }
    if (compare(bytes, e5, bytes, e4) < 0) { swap(e5, e4)
      if (compare(bytes, e4, bytes, e3) < 0) { swap(e4, e3)
        if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
          if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }
        }
      }
    }

    // Pointers
    var less  = left  // The index of the first element of center part
    var great = right // The index before the first element of right part

    if (compare(bytes, e1, bytes, e2) != 0 && compare(bytes, e2, bytes, e3) != 0 &&
        compare(bytes, e3, bytes, e4) != 0 && compare(bytes, e4, bytes, e5) != 0 ) {
      /*
       * Use the second and fourth of the five sorted elements as pivots.
       * These values are inexpensive approximations of the first and
       * second terciles of the array. Note that pivot1 <= pivot2.
       */
      val pivot1 = Array.ofDim[Byte](BIN_SIZE)
      System.arraycopy(bytes, e2, pivot1, 0, BIN_SIZE)
      val pivot2 = Array.ofDim[Byte](BIN_SIZE)
      System.arraycopy(bytes, e4, pivot2, 0, BIN_SIZE)

      /*
       * The first and the last elements to be sorted are moved to the
       * locations formerly occupied by the pivots. When partitioning
       * is complete, the pivots are swapped back into their final
       * positions, and excluded from subsequent sorting.
       */
      System.arraycopy(bytes, left, bytes, e2, BIN_SIZE)
      System.arraycopy(bytes, right, bytes, e4, BIN_SIZE)

      // Skip elements, which are less or greater than pivot values.
      while ({ less += BIN_SIZE; compare(bytes, less, pivot1, 0) < 0 }) {}
      while ({ great -= BIN_SIZE; compare(bytes, great, pivot2, 0) > 0 }) {}

      /*
       * Partitioning:
       *
       *   left part           center part                   right part
       * +--------------------------------------------------------------+
       * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
       * +--------------------------------------------------------------+
       *               ^                          ^       ^
       *               |                          |       |
       *              less                        k     great
       *
       * Invariants:
       *
       *              all in (left, less)   < pivot1
       *    pivot1 <= all in [less, k)     <= pivot2
       *              all in (great, right) > pivot2
       *
       * Pointer k is the first index of ?-part.
       */

      var k = less
      var loop = true
      while (k <= great && loop) {
        val ak = getThreadLocalChunk(bytes, k)
        if (compare(ak, 0, pivot1, 0) < 0) { // Move a[k] to left part
          System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
          System.arraycopy(ak, 0, bytes, less, BIN_SIZE)
          less += BIN_SIZE
        } else if (compare(ak, 0, pivot2, 0) > 0) { // Move a[k] to right part
          while (compare(bytes, great, pivot2, 0) > 0) {
            if (great == k) {
              loop = false
            }
            great -= BIN_SIZE
          }
          if (loop) {
            if (compare(bytes, great, pivot1, 0) < 0) { // a[great] <= pivot2
              System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
              System.arraycopy(bytes, great, bytes, less, BIN_SIZE)
              less += BIN_SIZE
            } else { // pivot1 <= a[great] <= pivot2
              System.arraycopy(bytes, great, bytes, k, BIN_SIZE)
            }
            System.arraycopy(ak, 0, bytes, great, BIN_SIZE)
            great -= BIN_SIZE
          }
        }
        k += BIN_SIZE
      }

      k = less
      loop = true
      while (k <= great && loop) {
        val ak = getThreadLocalChunk(bytes, k)
        if (compare(ak, 0, pivot1, 0) < 0) { // Move a[k] to left part
          System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
          System.arraycopy(ak, 0, bytes, less, BIN_SIZE)
          less += BIN_SIZE
        } else if (compare(ak, 0, pivot2, 0) > 0) { // Move a[k] to right part
          while (compare(bytes, great, pivot2, 0) > 0) {
            if (great == k) {
              loop = false
            }
            great -= BIN_SIZE
          }
          if (compare(bytes, great, pivot1, 0) < 0) { // a[great] <= pivot2
            System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
            System.arraycopy(bytes, great, bytes, less, BIN_SIZE)
            less += BIN_SIZE
          } else { // pivot1 <= a[great] <= pivot2
            System.arraycopy(bytes, great, bytes, k, BIN_SIZE)
          }
          System.arraycopy(ak, 0, bytes, great, BIN_SIZE)
          great -= BIN_SIZE
        }
        k += BIN_SIZE
      }

      // Swap pivots into their final positions
      System.arraycopy(bytes, less - BIN_SIZE, bytes, left, BIN_SIZE)
      System.arraycopy(pivot1, 0, bytes, less - BIN_SIZE, BIN_SIZE)
      System.arraycopy(bytes, great + BIN_SIZE, bytes, right, BIN_SIZE)
      System.arraycopy(pivot2, 0, bytes, great + BIN_SIZE, BIN_SIZE)

      // Sort left and right parts recursively, excluding known pivots
      quickSort(bytes, left, less - 2 * BIN_SIZE, leftmost)
      quickSort(bytes, great + 2 * BIN_SIZE, right, leftmost = false)

      /*
       * If center part is too large (comprises > 4/7 of the array),
       * swap internal pivot values to ends.
       */
      if (less < e1 && e5 < great) {
        // Skip elements, which are equal to pivot values.
        while (compare(bytes, less, pivot1, 0) == 0) { less += BIN_SIZE }
        while (compare(bytes, great, pivot2, 0) == 0) { great -= BIN_SIZE }

        /*
         * Partitioning:
         *
         *   left part         center part                  right part
         * +----------------------------------------------------------+
         * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
         * +----------------------------------------------------------+
         *              ^                        ^       ^
         *              |                        |       |
         *             less                      k     great
         *
         * Invariants:
         *
         *              all in (*,  less) == pivot1
         *     pivot1 < all in [less,  k)  < pivot2
         *              all in (great, *) == pivot2
         *
         * Pointer k is the first index of ?-part.
         */
        var k = less
        loop = true
        while (k <= great && loop) {
          val ak = getThreadLocalChunk(bytes, k)
          if (compare(ak, 0, pivot1, 0) == 0) { // Move a[k] to left part
            System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
            System.arraycopy(ak, 0, bytes, less, BIN_SIZE)
            less += BIN_SIZE
          } else if (compare(ak, 0, pivot2, 0) == 0) { // Move a[k] to right part
            while (compare(bytes, great, pivot2, 0) == 0) {
              if (great == k) {
                loop = false
              }
              great -= BIN_SIZE
            }
            if (compare(bytes, great, pivot1, 0) == 0) { // a[great] < pivot2
              System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
              System.arraycopy(pivot1, 0, bytes, less, BIN_SIZE)
              less += BIN_SIZE
            } else { // pivot1 < a[great] < pivot2
              System.arraycopy(bytes, great, bytes, k, BIN_SIZE)
            }
            System.arraycopy(ak, 0, bytes, great, BIN_SIZE)
            great -= BIN_SIZE
          }
          k += BIN_SIZE
        }
      }

      // Sort center part recursively
      quickSort(bytes, less, great, leftmost = false)

    } else { // Partitioning with one pivot

      /*
       * Use the third of the five sorted elements as pivot.
       * This value is inexpensive approximation of the median.
       */
      val pivot = Array.ofDim[Byte](BIN_SIZE)
      System.arraycopy(bytes, e3, pivot, 0, BIN_SIZE)

      /*
       * Partitioning degenerates to the traditional 3-way
       * (or "Dutch National Flag") schema:
       *
       *   left part    center part              right part
       * +-------------------------------------------------+
       * |  < pivot  |   == pivot   |     ?    |  > pivot  |
       * +-------------------------------------------------+
       *              ^              ^        ^
       *              |              |        |
       *             less            k      great
       *
       * Invariants:
       *
       *   all in (left, less)   < pivot
       *   all in [less, k)     == pivot
       *   all in (great, right) > pivot
       *
       * Pointer k is the first index of ?-part.
       */
      var k = less
      while (k <= great) {
        if (compare(bytes, k, pivot, 0) != 0) {
          val ak = getThreadLocalChunk(bytes, k)
          if (compare(ak, 0, pivot, 0) < 1) { // Move a[k] to left part
            System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
            System.arraycopy(ak, 0, bytes, less, BIN_SIZE)
            less += BIN_SIZE
          } else { // a[k] > pivot - Move a[k] to right part
            while (compare(bytes, great, pivot, 0) > 0) {
              great -= BIN_SIZE
            }
            if (compare(bytes, great, pivot, 0) < 0) { // a[great] <= pivot
              System.arraycopy(bytes, less, bytes, k, BIN_SIZE)
              System.arraycopy(bytes, great, bytes, less, BIN_SIZE)
              less += BIN_SIZE
            } else { // a[great] == pivot
              System.arraycopy(pivot, 0, bytes, k, BIN_SIZE)
            }
            System.arraycopy(ak, 0, bytes, great, BIN_SIZE)
            great -= BIN_SIZE
          }
        }
        k += BIN_SIZE
      }

      /*
       * Sort left and right parts recursively.
       * All elements from center part are equal
       * and, therefore, already sorted.
       */
      quickSort(bytes, left, less - BIN_SIZE, leftmost)
      quickSort(bytes, great + BIN_SIZE, right, leftmost = false)
    }
  }

  // take care - uses thread-local state
  private def getThreadLocalChunk(bytes: Array[Byte], offset: Int): Array[Byte] = {
    val chunk = swapBuffers.get()
    System.arraycopy(bytes, offset, chunk, 0, BIN_SIZE)
    chunk
  }
}