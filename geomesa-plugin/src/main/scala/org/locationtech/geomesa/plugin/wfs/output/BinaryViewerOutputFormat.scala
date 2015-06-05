/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.plugin.wfs.output

import java.io.{BufferedOutputStream, OutputStream}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Executors}

import com.typesafe.scalalogging.slf4j.Logging
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureCollection
import org.locationtech.geomesa.accumulo.iterators.{BinSorter, BinAggregatingIterator}
import org.locationtech.geomesa.utils.geotools.Conversions._

import scala.collection.JavaConversions._

/**
 * Output format for wfs requests that encodes features into a binary format.
 * To trigger, use outputFormat=application/vnd.binary-viewer in your wfs request
 *
 * Required flags:
 * format_options=trackId:<track_attribute_name>;
 *
 * @param gs
 */
class BinaryViewerOutputFormat(gs: GeoServer)
    extends WFSGetFeatureOutputFormat(gs, Set("bin", BinaryViewerOutputFormat.MIME_TYPE)) with Logging {

  import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator.BIN_SIZE
  import org.locationtech.geomesa.plugin.wfs.output.BinaryViewerOutputFormat._

  override def getMimeType(value: AnyRef, operation: Operation) = MIME_TYPE

  override def getPreferredDisposition(value: AnyRef, operation: Operation) = Response.DISPOSITION_INLINE

  override def getAttachmentFileName(value: AnyRef, operation: Operation) = {
    val gfr = GetFeatureRequest.adapt(operation.getParameters()(0))
    val name = Option(gfr.getHandle).getOrElse(gfr.getQueries.get(0).getTypeNames.get(0).getLocalPart)
    s"$name.$FILE_EXTENSION$BIN_SIZE"
  }

  override def write(featureCollections: FeatureCollectionResponse,
                     output: OutputStream,
                     getFeature: Operation): Unit = {

    // format_options flags for customizing the request
    val request = GetFeatureRequest.adapt(getFeature.getParameters()(0))
    val trackId = Option(request.getFormatOptions.get(TRACK_ID_FIELD).asInstanceOf[String]).getOrElse {
      throw new IllegalArgumentException(s"$TRACK_ID_FIELD is a required format option")
    }
    val sortFromRequest =
      Option(request.getFormatOptions.get(SORT_FIELD).asInstanceOf[String]).exists(_.toBoolean)

    val bos = new BufferedOutputStream(output)

    val sort = sortFromRequest || sys.props.getOrElse(SORT_SYS_PROP, DEFAULT_SORT).toBoolean
    val tserverSort = sort || sys.props.getOrElse(PARTIAL_SORT_SYS_PROP, DEFAULT_SORT).toBoolean

    featureCollections.getFeatures.zip(request.getQueries).foreach { case (fc, query) =>
      fc match {
        case a: AccumuloFeatureCollection =>
          // set query hints for optimizations in the accumulo iterators
          import org.locationtech.geomesa.accumulo.index.QueryHints._
          a.getQuery.getHints.put(BIN_TRACK_KEY, trackId)
          a.getQuery.getHints.put(BIN_SORT_KEY, tserverSort)
          // noinspection EmptyCheck
          if (a.getQuery.getSortBy != null && a.getQuery.getSortBy.length > 0) {
            // we don't want to sort - because we manipulate the sft coming back it would blow up
            logger.warn("Ignoring sort in bin request")
            a.getQuery.setSortBy(null)
          }

        case _ => // no optimization
          logger.warn("Non Accumulo feature collection found - bin request not optimized")
      }
      val iter = fc.asInstanceOf[SimpleFeatureCollection].features()
      val aggregates = iter.flatMap(f =>
        Option(f.getAttribute(BinAggregatingIterator.BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]))

      if (sort) {
        // we do some asynchronous pre-merging while we are waiting for all the data to come in
        // the pre-merging is expensive, as it merges in memory
        // the final merge doesn't have to allocate space for merging, as it writes directly to the output
        val numThreads = sys.props.getOrElse(SORT_THREADS_SYS_PROP, DEFAULT_SORT_THREADS).toInt
        val executor = Executors.newFixedThreadPool(numThreads)
        // access to this is manually synchronized so we can pull off 2 items at once
        val mergeQueue = collection.mutable.PriorityQueue.empty[Array[Byte]](new Ordering[Array[Byte]] {
          override def compare(x: Array[Byte], y: Array[Byte]) = y.length.compareTo(x.length) // shorter first
        })
        // holds buffers we don't want to consider anymore due to there size - also manually synchronized
        val doneMergeQueue = collection.mutable.ArrayBuffer.empty[Array[Byte]]
        val maxSizeToMerge = sys.props.getOrElse(SORT_HEAP_SYS_PROP, DEFAULT_SORT_HEAP).toInt
        val latch = new CountDownLatch(numThreads)
        val keepMerging = new AtomicBoolean(true)
        var i = 0
        while (i < numThreads) {
          executor.submit(new Runnable() {
            override def run() = {
              while (keepMerging.get()) {
                // pull out the 2 smallest items to merge
                // the final merge has to compare the first item in each buffer, so reducing the number of buffers helps
                val (left, right) = mergeQueue.synchronized {
                  if (mergeQueue.length > 1) (mergeQueue.dequeue(), mergeQueue.dequeue()) else (null, null)
                }
                if (left != null) { // right will also not be null
                  if (right.length > maxSizeToMerge) {
                    if (left.length > maxSizeToMerge) {
                      doneMergeQueue.synchronized(doneMergeQueue.append(left, right))
                    } else {
                      doneMergeQueue.synchronized(doneMergeQueue.append(right))
                      mergeQueue.synchronized(mergeQueue.enqueue(left))
                    }
                    Thread.sleep(10) // if we didn't find anything to merge, wait a bit before re-checking
                  } else {
                    val result = BinSorter.mergeSort(left, right)
                    mergeQueue.synchronized(mergeQueue.enqueue(result))
                  }
                } else {
                  // if we didn't find anything to merge, wait a bit before re-checking
                  Thread.sleep(10)
                }
              }
              latch.countDown() // indicate this thread is done
            }
          })
          i += 1
        }
        // queue up the aggregates coming in so that they can be processed by the merging threads above
        aggregates.foreach(a => mergeQueue.synchronized(mergeQueue.enqueue(a)))
        // once all data is back from the tservers, stop pre-merging and start streaming back to the client
        keepMerging.set(false)
        executor.shutdown() // this won't stop the threads, but will cleanup once they're done
        latch.await() // wait for the merge threads to finish
        // get an iterator that returns in sorted order
        val bins = BinSorter.mergeSort((doneMergeQueue ++ mergeQueue).iterator)
        while (bins.hasNext) {
          val (aggregate, offset) = bins.next()
          bos.write(aggregate, offset, BIN_SIZE)
        }
      } else {
        // no sort, just write directly to the output
        aggregates.foreach(bos.write)
      }
      iter.close()
      bos.flush()
    }
    // none of the implementations in geoserver call 'close' on the output stream
  }
}

object BinaryViewerOutputFormat extends Logging {

  val MIME_TYPE = "application/vnd.binary-viewer"
  val FILE_EXTENSION = "bin"
  val TRACK_ID_FIELD = "TRACKID"
  val SORT_FIELD     = "SORT"

  val SORT_SYS_PROP         = "geomesa.output.bin.sort"
  val PARTIAL_SORT_SYS_PROP = "geomesa.output.bin.sort.partial"
  val SORT_THREADS_SYS_PROP = "geomesa.output.bin.sort.threads"
  val SORT_HEAP_SYS_PROP    = "geomesa.output.bin.sort.memory"

  val DEFAULT_SORT = "false"
  val DEFAULT_SORT_THREADS = "2"
  val DEFAULT_SORT_HEAP = "2097152" // 2MB
}
