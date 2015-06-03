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
import org.geotools.factory.GeoTools
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.index.QueryPlanners._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
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
  var batchSize: Int = -1
  var binSize: Int = 16

  var topKey: Key = null
  var topValue: Value = new Value()
  var bytes: Array[Byte] = null
  var byteBuffer: ByteBuffer = null
  var currentRange: aRange = null

  var reusablesf: KryoBufferSimpleFeature = null

  var handleValue: () => Unit = null
  var writeBin: () => Unit = null
  var getTrackId: () => Int = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source.deepCopy(env)
    BinAggregatingIterator.initClassLoader(logger)

    sft = SimpleFeatureTypes.createType("test", options.get(SFT_OPT))
    filter = Option(options.get(CQL_OPT)).map(FastFilterFactory.toFilter).orNull
    batchSize = options.get(BATCH_SIZE_OPT).toInt

    geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    dtgIndex = options.get(DATE_OPT).toInt
    trackIndex = options.get(TRACK_OPT).toInt

    if (Option(options.get(BIN_CF_OPT)).exists(_.toBoolean)) {
      handleValue = if (filter == null) {
        () => {
          topKey = source.getTopKey
          val bin = source.getTopValue.get()
          System.arraycopy(bin, 0, bytes, byteBuffer.position, bin.length)
          byteBuffer.position(byteBuffer.position + bin.length)
        }
      } else {
        val sf = new ScalaSimpleFeature("", sft)
        val gf = new GeometryFactory
        () => {
          parseBinValue(sf, gf)
          if (filter.evaluate(sf)) {
            topKey = source.getTopKey
            System.arraycopy(source.getTopValue.get, 0, bytes, byteBuffer.position, binSize)
            byteBuffer.position(byteBuffer.position + binSize)
          }
        }
      }
    } else {
      reusablesf = new KryoFeatureSerializer(sft).getReusableFeature
      getTrackId = if (trackIndex == -1) {
        () => reusablesf.getID.hashCode()
      } else {
        () => reusablesf.getAttribute(trackIndex).hashCode()
      }
      writeBin = if (sft.getGeometryDescriptor.getType.getBinding == classOf[Point] ||
          sft.getGeometryDescriptor.getType.getBinding == classOf[LineString]) {
        writePoint
      } else {
        writeGeometry
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

    bytes = Array.ofDim(binSize * batchSize)
    byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
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
    val maxBytes = batchSize * binSize
    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey) && byteBuffer.position < maxBytes) {
      handleValue()
      // Advance the source iterator
      source.next()
    }

    if (byteBuffer.position == 0) {
      topKey = null
      topValue = null
    } else {
      if (topValue == null) {
        topValue = new Value()
      }
      sortByChunks(bytes, byteBuffer.position, binSize)
      if (byteBuffer.position == maxBytes) {
        // use the existing buffer if possible
        topValue.set(bytes)
      } else {
        // if not, we have to copy it
        val copy = Array.ofDim[Byte](byteBuffer.position)
        System.arraycopy(bytes, 0, copy, 0, byteBuffer.position)
        topValue.set(copy)
      }
    }
  }

  private def writePoint(pt: Point): Unit = {
    byteBuffer.putInt(getTrackId())
    byteBuffer.putInt((reusablesf.getDateAsLong(dtgIndex) / 1000).toInt)
    byteBuffer.putFloat(pt.getY.toFloat) // y is lat
    byteBuffer.putFloat(pt.getX.toFloat) // x is lon
  }

  def writePoint(): Unit = writePoint(reusablesf.getAttribute(geomIndex).asInstanceOf[Point])

  def writeLineString(): Unit = {
    val geom = reusablesf.getAttribute(geomIndex).asInstanceOf[LineString]
    var i = 0
    while (i < geom.getNumPoints) {
      writePoint(geom.getPointN(i))
      i += 1
    }
  }

  def writeGeometry(): Unit =
    writePoint(reusablesf.getAttribute(geomIndex).asInstanceOf[Geometry].getInteriorPoint)

  def parseBinValue(sf: ScalaSimpleFeature, gf: GeometryFactory): Unit = {
    val values = Convert2ViewerFunction.decode(source.getTopValue.get)
    sf.setAttribute(geomIndex, gf.createPoint(new Coordinate(values.lat, values.lon)))
    sf.setAttribute(trackIndex, values.trackId.orNull)
    sf.setAttribute(dtgIndex, new Date(values.dtg))
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object BinAggregatingIterator extends Logging {

  // need to be lazy to avoid class loading issues before init is called
  lazy val BIN_SFT = SimpleFeatureTypes.createType("bin", "bin:String,*geom:Point:srid=4326")
  lazy val BIN_ATTRIBUTE_INDEX = BIN_SFT.indexOf("bin")
  private lazy val zeroPoint = WKTUtils.read("POINT(0 0)")
  private var initialized = false

  val BATCH_SIZE_SYS_PROP = "org.locationtech.geomesa.bin.batch.size"

  val SFT_OPT        = "sft"
  val CQL_OPT        = "cql"
  val BATCH_SIZE_OPT = "batch"

  val BIN_CF_OPT     = "bincf"
  val TRACK_OPT      = "track"
  val DATE_OPT       = "date"

  def configurePrecomputed(sft: SimpleFeatureType,
                           filter: Option[Filter],
                           priority: Int): IteratorSetting = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val trackId = sft.getBinTrackId.getOrElse("id")
    val dtg = org.locationtech.geomesa.accumulo.index.getDtgFieldName(sft).getOrElse {
      throw new RuntimeException(s"No default dtg field found in SFT $sft")
    }
    val is = configureDynamic(sft, filter, trackId, dtg, priority)
    is.addOption(BIN_CF_OPT, "true")
    is
  }

  def configureDynamic(sft: SimpleFeatureType,
                       filter: Option[Filter],
                       trackId: String,
                       dtg: String,
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

  def getBatchSize: Int =
    Option(System.getProperty(BATCH_SIZE_SYS_PROP)).map(_.toInt).getOrElse(65536)// 1MB for 16 byte bins

  private val chunkOrdering = new Ordering[Array[Byte]]() {
    override def compare(x: Array[Byte], y: Array[Byte]) = BinAggregatingIterator.compare(x, 0, y, 0)
  }

  private val priorityOrdering = new Ordering[(Array[Byte], Int)]() {
    override def compare(x: (Array[Byte], Int), y: (Array[Byte], Int)) =
      BinAggregatingIterator.compare(y._1, y._2, x._1, x._2) // reverse for priority queue
  }

  /**
   * Sorts an aggregate in place
   *
   * @param aggregate
   * @param length
   * @param chunkSize
   */
  def sortByChunks(aggregate: Array[Byte], length: Int, chunkSize: Int): Unit = {
    // TODO ideally we could do an in-place quicksort
    val sorted = aggregate.grouped(chunkSize).take(length / chunkSize).toArray.sorted(chunkOrdering)
    var i = 0
    while (i < sorted.length) {
      System.arraycopy(sorted(i), 0, aggregate, i * chunkSize, chunkSize)
      i += 1
    }
  }

  /**
   * Takes a series of minor (already sorted) aggregates and combines them in a final sort
   *
   * @param aggregates
   * @param chunkSize
   * @return
   */
  def mergeSort(aggregates: Iterator[Array[Byte]], chunkSize: Int): Iterator[(Array[Byte], Int)] = {
    val queue = new scala.collection.mutable.PriorityQueue[(Array[Byte], Int)]()(priorityOrdering)
    val sizes = scala.collection.mutable.ArrayBuffer.empty[Int]
    while (aggregates.hasNext) {
      val next = aggregates.next()
      // TODO remove this check
      val dtgs = next.grouped(chunkSize).map(Convert2ViewerFunction.decode(_).dtg)
      if (dtgs.reduceLeft((l, r) => if (l <= r) r else Long.MaxValue) == Long.MaxValue) {
        println("found invalid chunk")
      }
      sizes.append(next.length / chunkSize)
      queue.enqueue((next, 0))
    }
    logger.debug(s"Got back ${queue.length} aggregates with an average size of ${sizes.sum / sizes.length}" +
        s" chunks and a median size of ${sizes.sorted.apply(sizes.length / 2)} chunks")
    println(s"Got back ${queue.length} aggregates with an average size of ${sizes.sum / sizes.length}" +
        s" chunks and a median size of ${sizes.sorted.apply(sizes.length / 2)} chunks") // TODO remove this
    new Iterator[(Array[Byte], Int)] {
      override def hasNext = queue.nonEmpty
      override def next() = {
        val (aggregate, offset) = queue.dequeue()
        if (offset < aggregate.length - chunkSize) {
          queue.enqueue((aggregate, offset + chunkSize))
        }
        (aggregate, offset)
      }
    }
  }

  /**
   * Compares two logical chunks by date
   *
   * @param left
   * @param leftOffset index of the chunk (not index into the array)
   * @param right
   * @param rightOffset index of the chunk (not index into the array)
   * @return
   */
  def compare(left: Array[Byte], leftOffset: Int, right: Array[Byte], rightOffset: Int): Int =
    compareIntLittleEndian(left, leftOffset + 4, right, rightOffset + 4)

  /**
   * Comparison based on the integer encoding used by ByteBuffer
   * original code is in private/proected java.nio packages
   */
  def compareIntLittleEndian(left: Array[Byte], leftOffset: Int, right: Array[Byte], rightOffset: Int): Int = {
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

  def getChunk(bytes: Array[Byte], offset: Int, chunkSize: Int): Array[Byte] = {
    val chunk = Array.ofDim[Byte](chunkSize)
    System.arraycopy(bytes, offset, chunk, 0, chunkSize)
    chunk
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

  //  /**
  //   * If the length of an array to be sorted is less than this
  //   * constant, insertion sort is used in preference to Quicksort.
  //   */
  //  private val INSERTION_SORT_THRESHOLD: Int = 47
  //
  //  /**
  //   * Sorts the specified range of the array by Dual-Pivot Quicksort.
  //   * Modified version of java's DualPivotQuicksort
  //   *
  //   * @param bytes the array to be sorted
  //   * @param oleft the index of the first element, inclusive, to be sorted
  //   * @param oright the index of the last element, inclusive, to be sorted
  //   * @param leftmost indicates if this part is the leftmost in the range
  //   * @param chunkSize size of each bin record in the array
  //   */
  //  def sort(bytes: Array[Byte], oleft: Int, oright: Int, leftmost: Boolean, chunkSize: Int): Unit = {
  //
  //    var left = oleft
  //    var right = oright
  //    val length = right - left + 1
  //
  //    // Use insertion sort on tiny arrays
  //    if (length < INSERTION_SORT_THRESHOLD) {
  //      if (leftmost) {
  //        /*
  //         * Traditional (without sentinel) insertion sort,
  //         * optimized for server VM, is used in case of
  //         * the leftmost part.
  //         */
  //        var i = left
  //        var j = i
  //        while (i < right) {
  //          val chunki = getChunk(bytes, i + 1, chunkSize)
  //          while (j >= left && compare(chunki, 0, bytes, j, chunkSize) < 0) {
  //            // TODO fix other compares, extract copy method?
  //            System.arraycopy(bytes, j * chunkSize, bytes, (j + 1) * chunkSize, chunkSize)
  //            j -= 1
  //          }
  //          System.arraycopy(chunki, 0, bytes, (j + 1) * chunkSize, chunkSize)
  //          i += 1
  //          j = i
  //        }
  //        //  for (int i = left, j = i; i < right; j = ++i) {
  //        //    int ai = a[i + 1];
  //        //    while (ai < a[j]) {
  //        //      a[j + 1] = a[j];
  //        //      if (j-- == left) {
  //        //        break;
  //        //      }
  //        //    }
  //        //    a[j + 1] = ai;
  //        //  }
  //      } else {
  //        /*
  //         * Skip the longest ascending sequence.
  //         */
  //        do {
  //          if (left >= right) {
  //            return
  //          }
  //        } while ({ left += 1; compare(bytes, left * chunkSize, bytes, (left - 1) * chunkSize, chunkSize) >= 0 })
  //        //  do {
  //        //    if (left >= right) {
  //        //      return;
  //        //    }
  //        //  } while (a[++left] >= a[left - 1]);
  //
  //        /*
  //         * Every element from adjoining part plays the role
  //         * of sentinel, therefore this allows us to avoid the
  //         * left range check on each iteration. Moreover, we use
  //         * the more optimized algorithm, so called pair insertion
  //         * sort, which is faster (in the context of Quicksort)
  //         * than traditional implementation of insertion sort.
  //         */
  //
  //        var k: Int = left
  //        while ({ left += 1; left } <= right) {
  //          var a1 = getChunk(bytes, k * chunkSize, chunkSize)
  //          var a2: Int = a(left)
  //          if (a1 < a2) {
  //            a2 = a1
  //            a1 = a(left)
  //          }
  //          while (a1 < a({k -= 1; k})) {
  //            a(k + 2) = a(k)
  //          }
  //          a({k += 1; k} + 1) = a1
  //          while (a2 < a({k -= 1; k})) {
  //            a(k + 1) = a(k)
  //          }
  //          a(k + 1) = a2
  //
  //          left += 1
  //          k = left
  //        }
  //
  //        for (int k = left; ++left <= right; k = ++left) {
  //            int a1 = a[k], a2 = a[left];
  //
  //            if (a1 < a2) {
  //                a2 = a1; a1 = a[left];
  //            }
  //            while (a1 < a[--k]) {
  //                a[k + 2] = a[k];
  //            }
  //            a[++k + 1] = a1;
  //
  //            while (a2 < a[--k]) {
  //                a[k + 1] = a[k];
  //            }
  //            a[k + 1] = a2;
  //          }
  //          int last = a[right];
  //
  //          while (last < a[--right]) {
  //              a[right + 1] = a[right];
  //          }
  //          a[right + 1] = last;
  //      }
  //      return
  //    }
  //
  //        // Inexpensive approximation of length / 7
  //        int seventh = (length >> 3) + (length >> 6) + 1;
  //
  //        /*
  //         * Sort five evenly spaced elements around (and including) the
  //         * center element in the range. These elements will be used for
  //         * pivot selection as described below. The choice for spacing
  //         * these elements was empirically determined to work well on
  //         * a wide variety of inputs.
  //         */
  //        int e3 = (left + right) >>> 1; // The midpoint
  //        int e2 = e3 - seventh;
  //        int e1 = e2 - seventh;
  //        int e4 = e3 + seventh;
  //        int e5 = e4 + seventh;
  //
  //        // Sort these elements using insertion sort
  //        if (a[e2] < a[e1]) { int t = a[e2]; a[e2] = a[e1]; a[e1] = t; }
  //
  //        if (a[e3] < a[e2]) { int t = a[e3]; a[e3] = a[e2]; a[e2] = t;
  //            if (t < a[e1]) { a[e2] = a[e1]; a[e1] = t; }
  //        }
  //        if (a[e4] < a[e3]) { int t = a[e4]; a[e4] = a[e3]; a[e3] = t;
  //            if (t < a[e2]) { a[e3] = a[e2]; a[e2] = t;
  //                if (t < a[e1]) { a[e2] = a[e1]; a[e1] = t; }
  //            }
  //        }
  //        if (a[e5] < a[e4]) { int t = a[e5]; a[e5] = a[e4]; a[e4] = t;
  //            if (t < a[e3]) { a[e4] = a[e3]; a[e3] = t;
  //                if (t < a[e2]) { a[e3] = a[e2]; a[e2] = t;
  //                    if (t < a[e1]) { a[e2] = a[e1]; a[e1] = t; }
  //                }
  //            }
  //        }
  //
  //        // Pointers
  //        int less  = left;  // The index of the first element of center part
  //        int great = right; // The index before the first element of right part
  //
  //        if (a[e1] != a[e2] && a[e2] != a[e3] && a[e3] != a[e4] && a[e4] != a[e5]) {
  //            /*
  //             * Use the second and fourth of the five sorted elements as pivots.
  //             * These values are inexpensive approximations of the first and
  //             * second terciles of the array. Note that pivot1 <= pivot2.
  //             */
  //            int pivot1 = a[e2];
  //            int pivot2 = a[e4];
  //
  //            /*
  //             * The first and the last elements to be sorted are moved to the
  //             * locations formerly occupied by the pivots. When partitioning
  //             * is complete, the pivots are swapped back into their final
  //             * positions, and excluded from subsequent sorting.
  //             */
  //            a[e2] = a[left];
  //            a[e4] = a[right];
  //
  //            /*
  //             * Skip elements, which are less or greater than pivot values.
  //             */
  //            while (a[++less] < pivot1);
  //            while (a[--great] > pivot2);
  //
  //            /*
  //             * Partitioning:
  //             *
  //             *   left part           center part                   right part
  //             * +--------------------------------------------------------------+
  //             * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
  //             * +--------------------------------------------------------------+
  //             *               ^                          ^       ^
  //             *               |                          |       |
  //             *              less                        k     great
  //             *
  //             * Invariants:
  //             *
  //             *              all in (left, less)   < pivot1
  //             *    pivot1 <= all in [less, k)     <= pivot2
  //             *              all in (great, right) > pivot2
  //             *
  //             * Pointer k is the first index of ?-part.
  //             */
  //            outer:
  //            for (int k = less - 1; ++k <= great; ) {
  //                int ak = a[k];
  //                if (ak < pivot1) { // Move a[k] to left part
  //                    a[k] = a[less];
  //                    /*
  //                     * Here and below we use "a[i] = b; i++;" instead
  //                     * of "a[i++] = b;" due to performance issue.
  //                     */
  //                    a[less] = ak;
  //                    ++less;
  //                } else if (ak > pivot2) { // Move a[k] to right part
  //                    while (a[great] > pivot2) {
  //                        if (great-- == k) {
  //                            break outer;
  //                        }
  //                    }
  //                    if (a[great] < pivot1) { // a[great] <= pivot2
  //                        a[k] = a[less];
  //                        a[less] = a[great];
  //                        ++less;
  //                    } else { // pivot1 <= a[great] <= pivot2
  //                        a[k] = a[great];
  //                    }
  //                    /*
  //                     * Here and below we use "a[i] = b; i--;" instead
  //                     * of "a[i--] = b;" due to performance issue.
  //                     */
  //                    a[great] = ak;
  //                    --great;
  //                }
  //            }
  //
  //            // Swap pivots into their final positions
  //            a[left]  = a[less  - 1]; a[less  - 1] = pivot1;
  //            a[right] = a[great + 1]; a[great + 1] = pivot2;
  //
  //            // Sort left and right parts recursively, excluding known pivots
  //            sort(a, left, less - 2, leftmost);
  //            sort(a, great + 2, right, false);
  //
  //            /*
  //             * If center part is too large (comprises > 4/7 of the array),
  //             * swap internal pivot values to ends.
  //             */
  //            if (less < e1 && e5 < great) {
  //                /*
  //                 * Skip elements, which are equal to pivot values.
  //                 */
  //                while (a[less] == pivot1) {
  //                    ++less;
  //                }
  //
  //                while (a[great] == pivot2) {
  //                    --great;
  //                }
  //
  //                /*
  //                 * Partitioning:
  //                 *
  //                 *   left part         center part                  right part
  //                 * +----------------------------------------------------------+
  //                 * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
  //                 * +----------------------------------------------------------+
  //                 *              ^                        ^       ^
  //                 *              |                        |       |
  //                 *             less                      k     great
  //                 *
  //                 * Invariants:
  //                 *
  //                 *              all in (*,  less) == pivot1
  //                 *     pivot1 < all in [less,  k)  < pivot2
  //                 *              all in (great, *) == pivot2
  //                 *
  //                 * Pointer k is the first index of ?-part.
  //                 */
  //                outer:
  //                for (int k = less - 1; ++k <= great; ) {
  //                    int ak = a[k];
  //                    if (ak == pivot1) { // Move a[k] to left part
  //                        a[k] = a[less];
  //                        a[less] = ak;
  //                        ++less;
  //                    } else if (ak == pivot2) { // Move a[k] to right part
  //                        while (a[great] == pivot2) {
  //                            if (great-- == k) {
  //                                break outer;
  //                            }
  //                        }
  //                        if (a[great] == pivot1) { // a[great] < pivot2
  //                            a[k] = a[less];
  //                            /*
  //                             * Even though a[great] equals to pivot1, the
  //                             * assignment a[less] = pivot1 may be incorrect,
  //                             * if a[great] and pivot1 are floating-point zeros
  //                             * of different signs. Therefore in float and
  //                             * double sorting methods we have to use more
  //                             * accurate assignment a[less] = a[great].
  //                             */
  //                            a[less] = pivot1;
  //                            ++less;
  //                        } else { // pivot1 < a[great] < pivot2
  //                            a[k] = a[great];
  //                        }
  //                        a[great] = ak;
  //                        --great;
  //                    }
  //                }
  //            }
  //
  //            // Sort center part recursively
  //            sort(a, less, great, false);
  //
  //        } else { // Partitioning with one pivot
  //            /*
  //             * Use the third of the five sorted elements as pivot.
  //             * This value is inexpensive approximation of the median.
  //             */
  //            int pivot = a[e3];
  //
  //            /*
  //             * Partitioning degenerates to the traditional 3-way
  //             * (or "Dutch National Flag") schema:
  //             *
  //             *   left part    center part              right part
  //             * +-------------------------------------------------+
  //             * |  < pivot  |   == pivot   |     ?    |  > pivot  |
  //             * +-------------------------------------------------+
  //             *              ^              ^        ^
  //             *              |              |        |
  //             *             less            k      great
  //             *
  //             * Invariants:
  //             *
  //             *   all in (left, less)   < pivot
  //             *   all in [less, k)     == pivot
  //             *   all in (great, right) > pivot
  //             *
  //             * Pointer k is the first index of ?-part.
  //             */
  //            for (int k = less; k <= great; ++k) {
  //                if (a[k] == pivot) {
  //                    continue;
  //                }
  //                int ak = a[k];
  //                if (ak < pivot) { // Move a[k] to left part
  //                    a[k] = a[less];
  //                    a[less] = ak;
  //                    ++less;
  //                } else { // a[k] > pivot - Move a[k] to right part
  //                    while (a[great] > pivot) {
  //                        --great;
  //                    }
  //                    if (a[great] < pivot) { // a[great] <= pivot
  //                        a[k] = a[less];
  //                        a[less] = a[great];
  //                        ++less;
  //                    } else { // a[great] == pivot
  //                        /*
  //                         * Even though a[great] equals to pivot, the
  //                         * assignment a[k] = pivot may be incorrect,
  //                         * if a[great] and pivot are floating-point
  //                         * zeros of different signs. Therefore in float
  //                         * and double sorting methods we have to use
  //                         * more accurate assignment a[k] = a[great].
  //                         */
  //                        a[k] = pivot;
  //                    }
  //                    a[great] = ak;
  //                    --great;
  //                }
  //            }
  //
  //            /*
  //             * Sort left and right parts recursively.
  //             * All elements from center part are equal
  //             * and, therefore, already sorted.
  //             */
  //            sort(a, left, less - 1, leftmost);
  //            sort(a, great + 1, right, false);
  //        }
  //    }
}