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

import java.util.{HashSet => JHashSet, Date}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.index._

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
    with Logging {

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
    source.seek(range, columnFamilies, inclusive)
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
    while (topValue.isEmpty && source.hasTop) {
     // TODO optimize transform/decode
      val value = source.getTopValue
      lazy val feature = featureDecoder.decode(value)

      val meetsFilters = checkUniqueId.forall(fn => fn(feature.getID)) &&
          stFilter.forall(fn => fn(feature.getDefaultGeometry.asInstanceOf[Geometry],
            dateAttributeName.flatMap(d => Option(feature.getAttribute(d).asInstanceOf[Date])).map(_.getTime))) &&
          ecqlFilter.forall(fn => fn(feature))

      if (meetsFilters) {
        // update the key and value
        topKey = Some(source.getTopKey)
        // apply any transform here
        topValue = transform.map(fn => new Value(fn(feature))).orElse(Some(value))
      }

      // increment the underlying iterator
      source.next()
    }
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("SpatioTemporalIntersectingIterator does not support deepCopy")
}
