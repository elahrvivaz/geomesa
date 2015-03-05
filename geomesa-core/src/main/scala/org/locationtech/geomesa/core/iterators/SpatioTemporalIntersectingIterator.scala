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

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.index
import org.locationtech.geomesa.utils.stats.MethodProfiling

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
    extends GeomesaFilteringIterator
    with HasFeatureType
    with SetTopUnique
    with SetTopFilterUnique
    with SetTopStFilterUnique
    with SetTopTransformUnique
    with SetTopFilterTransformUnique
    with SetTopStFilterTransformUnique {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    // stFilter and filter should be mutually exclusive
    setTopOptimized = (stFilter, filter, transform, checkUniqueId) match {
      case (null, null, null, null) => setTopInclude
      case (null, null, null, _)    => setTopUnique
      case (null, _, null, null)    => setTopFilter
      case (null, _, null, _)       => setTopFilterUnique
      case (_, null, null, null)    => setTopStFilter
      case (_, null, null, _)       => setTopStFilterUnique
      case (null, null, _, null)    => setTopTransform
      case (null, null, _, _)       => setTopTransformUnique
      case (null, _, _, null)       => setTopFilterTransform
      case (null, _, _, _)          => setTopFilterTransformUnique
      case (_, null, _, null)       => setTopStFilterTransform
      case (_, null, _, _)          => setTopStFilterTransformUnique
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)
}
