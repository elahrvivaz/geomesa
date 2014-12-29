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
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.stats.{NoOpTimings, MethodProfiling, AutoLoggingTimings, Timings}

/**
 * Remember that we maintain two sources:  one for index records (that determines
 * whether the point is inside the search polygon), and one for data records (that
 * can consist of multiple data rows per index row, one data-row per attribute).
 * Each time the index source advances, we need to reposition the data source.
 *
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
  import SpatioTemporalIntersectingIterator.noOpTimings

  var topKey: Option[Key] = None
  var topValue: Option[Value] = None
  var source: SortedKeyValueIterator[Key, Value] = null

  override def init(
      source: SortedKeyValueIterator[Key, Value],
      options: java.util.Map[String, String],
      env: IteratorEnvironment) = {

    TServerClassLoader.initClassLoader(logger)
    initFeatureType(options)
    init(featureType, options)
    this.source = source.deepCopy(env)
  }

  override def hasTop = topKey.isDefined

  override def getTopKey = topKey.orNull

  override def getTopValue = topValue.orNull

  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    // move the source iterator to the right starting spot
    profile(source.seek(range, columnFamilies, inclusive), "source.seek")
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
    while (topValue.isEmpty && profile(source.hasTop, "source.hasTop")) {

      val indexKey = profile(source.getTopKey, "source.getTopKey")

      if (SpatioTemporalTable.isIndexEntry(indexKey)) { // if this is a data entry, skip it
        // only decode it if we have a filter to evaluate
        // the value contains the full-resolution geometry and time plus feature ID
        lazy val decodedValue = profile(indexEncoder.decode(source.getTopValue.get), "decodeIndexValue")

        // evaluate the filter checks, in least to most expensive order
        val meetsIndexFilters = profile(checkUniqueId.forall(fn => fn(decodedValue.id)), "checkUniqueId") &&
            profile(stFilter.forall(fn => fn(decodedValue.geom, decodedValue.date.map(_.getTime))), "stFilter")

        if (meetsIndexFilters) { // we hit a valid geometry, date and id
          // we increment the source iterator, which should point to a data entry
          profile(source.next(), "source.next")
          if (profile(source.hasTop, "source.hasTop")) {
            val dataKey = profile(source.getTopKey, "source.getTopKey")
            if (SpatioTemporalTable.isDataEntry(dataKey)) {
              val dataValue = profile(source.getTopValue, "source.getTopValue")
              lazy val decodedFeature = profile(featureDecoder.decode(dataValue), "decodeFeature")
              val meetsEcqlFilter = profile(ecqlFilter.forall(fn => fn(decodedFeature)), "ecqlFilter")
              if (meetsEcqlFilter) {
                // update the key and value
                topKey = Some(dataKey)
                // apply any transform here
                topValue = profile(transform.map(fn => new Value(fn(decodedFeature))), "transform")
                    .orElse(Some(dataValue))
              }
            } else {
              logger.error(s"Could not find the data key corresponding to index key '$indexKey' - " +
                  "there is no data entry.")
            }
          } else {
            logger.error(s"Could not find the data key corresponding to index key '$indexKey' - " +
                "there are no more entries")
          }
        }
        // TODO we have a lot of nested ifs here, try to clean it up
      }

      // increment the underlying iterator
      profile(source.next(), "source.next")
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("SpatioTemporalIntersectingIterator does not support deepCopy")
}

object SpatioTemporalIntersectingIterator {
  implicit val timings: Timings = new AutoLoggingTimings()
  implicit val noOpTimings: Timings = new NoOpTimings()
}