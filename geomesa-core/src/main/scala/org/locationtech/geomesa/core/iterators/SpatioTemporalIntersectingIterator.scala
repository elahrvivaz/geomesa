/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.utils.stats.{AutoLoggingTimings, MethodProfiling, NoOpTimings, Timings}

import scala.annotation.tailrec

/**
 * This iterator returns as its nextKey and nextValue responses the key and value
 * from the DATA iterator, not from the INDEX iterator.  The assumption is that
 * the data rows are what we care about; that we do not care about the index
 * rows that merely helped us find the data rows quickly.
 *
 * The other trick to remember about iterators is that they essentially pre-fetch
 * data.  "hasNext" really means, "was there a next record that you already found".
 */
class SpatioTemporalIntersectingIterator
    extends HasIteratorExtensions
    with SortedKeyValueIterator[Key, Value]
    with HasFeatureType
    with HasFeatureDecoder
    with HasSpatioTemporalFilter
    with HasEcqlFilter
    with HasTransforms
    with HasInMemoryDeduplication
    with MethodProfiling
    with Logging {

  // replace this with 'timings' to enable profile logging
  import org.locationtech.geomesa.core.iterators.SpatioTemporalIntersectingIterator.noOpTimings

  var topKey: Option[Key] = None
  var topValue: Option[Value] = None
  var indexSource: SortedKeyValueIterator[Key, Value] = null
  var dataSource: SortedKeyValueIterator[Key, Value] = null

  override def init(
      source: SortedKeyValueIterator[Key, Value],
      options: java.util.Map[String, String],
      env: IteratorEnvironment) = {

    TServerClassLoader.initClassLoader(logger)
    initFeatureType(options)
    init(featureType, options)
    this.indexSource = source.deepCopy(env)
    this.dataSource = source.deepCopy(env)
  }

  override def hasTop = topKey.isDefined

  override def getTopKey = topKey.orNull

  override def getTopValue = topValue.orNull

  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    import scala.collection.JavaConversions._
    // move the source iterator to the right starting spot
    profile(indexSource.seek(range, columnFamilies, inclusive), "source.seek")
    val cfs = columnFamilies.map(cf => new ArrayByteSequence(SpatioTemporalTable.INDEX_CF_PREFIX ++ cf.getBackingArray))
    profile(dataSource.seek(range, cfs, inclusive), "data-source.seek")
    findTop()
  }

  override def next() = findTop()

  /**
   * Advances the index-iterator to the next qualifying entry
   */
  def findTop() {

    // clear out the reference to the last entry
    topKey = None
    topValue = None

    // loop while there is more data and we haven't matched our filter
    while (topValue.isEmpty && profile(indexSource.hasTop && dataSource.hasTop, "source.hasTop")) {

      val indexKeyOption = profile(findNextIndexEntry(), "source.getTopKey")

      // if this is a data entry, skip it
      indexKeyOption.foreach { indexKey =>
        // only decode it if we have a filter to evaluate
        // the value contains the full-resolution geometry and time plus feature ID
        lazy val decodedValue = profile(indexEncoder.decode(indexSource.getTopValue.get), "decodeIndexValue")

        // evaluate the filter checks, in least to most expensive order
        val meetsIndexFilters = profile(checkUniqueId.forall(fn => fn(decodedValue.id)), "checkUniqueId") &&
            profile(stFilter.forall(fn => fn(decodedValue.geom, decodedValue.date.map(_.getTime))), "stFilter")

        if (meetsIndexFilters) { // we hit a valid geometry, date and id
          // we increment the source iterator, which should point to a data entry
//          if (profile(, "data-source.hasTop")) {
            val dataKeyOption = profile(findNextDataEntry(), "data-source.getTopKey")
            dataKeyOption.foreach { dataKey =>
              val dataValue = profile(dataSource.getTopValue, "source.getTopValue")
              lazy val decodedFeature = profile(featureDecoder.decode(dataValue), "decodeFeature")
              val meetsEcqlFilter = profile(ecqlFilter.forall(fn => fn(decodedFeature)), "ecqlFilter")
              if (meetsEcqlFilter) {
                // update the key and value
                topKey = Some(indexKey)
                // apply any transform here
                topValue = profile(transform.map(fn => new Value(fn(decodedFeature))), "transform")
                    .orElse(Some(dataValue))
              }
            } //else {
//              logger.error(s"Could not find the data key to index key '  $indexKey' - " +
//                  "there is no data entry.")
//            }
//          } else {
//            logger.error(s"Could not find the data key corresponding to index key '$indexKey' - " +
//                "there are no more entries")
//          }
        }
        // TODO we have a lot of nested ifs here, try to clean it up
      }

      // increment the underlying iterator
      profile(if (indexSource.hasTop) indexSource.next(), "source.next")
      profile(if (dataSource.hasTop) dataSource.next(), "data-source.next")
    }
  }

  @tailrec
  private def findNextIndexEntry(): Option[Key] = {
    val key = indexSource.getTopKey
    if (SpatioTemporalTable.isIndexEntry(key)) {
      Some(key)
    } else {
      indexSource.next()
      if (indexSource.hasTop) {
        findNextIndexEntry()
      } else {
        None
      }
    }
  }

  @tailrec
  private def findNextDataEntry(): Option[Key] = {
    val key = dataSource.getTopKey
    if (SpatioTemporalTable.isDataEntry(key)) {
      Some(key)
    } else {
      dataSource.next()
      if (dataSource.hasTop) {
        findNextDataEntry()
      } else {
        None
      }
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("SpatioTemporalIntersectingIterator does not support deepCopy")
}

object SpatioTemporalIntersectingIterator {
  implicit val timings: Timings = new AutoLoggingTimings()
  implicit val noOpTimings: Timings = new NoOpTimings()
}