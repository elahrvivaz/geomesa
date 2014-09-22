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

package org.locationtech.geomesa.core.iterators

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.DateTime
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try

/**
 * This is an Attribute Index Only Iterator. It should be used to avoid a join on the records table
 * in cases where only the geom, dtg and attribute in question are needed.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class AttributeIndexIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import org.locationtech.geomesa.core._

  var indexSource: SortedKeyValueIterator[Key, Value] = null

  var nextKey: Option[Key] = None
  var nextValue: Option[Value] = None
  var topKey: Option[Key] = None
  var topValue: Option[Value] = None

  var dtgFieldName: Option[String] = None
  var attributeRowPrefix: String = null
  var featureBuilder: SimpleFeatureBuilder = null
  var featureEncoder: SimpleFeatureEncoder = null

  var filterTest: (Geometry, Option[Long]) => Boolean = (_, _) => true

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {

    TServerClassLoader.initClassLoader(logger)

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    val featureType = SimpleFeatureTypes.createType(getClass.getCanonicalName, simpleFeatureTypeSpec)
    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    dtgFieldName = getDtgFieldName(featureType)
    attributeRowPrefix = index.getTableSharingPrefix(featureType)

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    featureEncoder = SimpleFeatureEncoderFactory.createEncoder(featureType, encodingOpt)

    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(featureType)

    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME)) {
      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      val sfb = new SimpleFeatureBuilder(featureType)

      val filter = ECQL.toFilter(filterString)
      val testFeature = sfb.buildFeature("test")

      filterTest = (geom: Geometry, odate: Option[Long]) => {
        testFeature.setDefaultGeometry(geom)
        dtgFieldName.foreach(dtgField => odate.foreach(date => testFeature.setAttribute(dtgField, new Date(date))))
        filter.evaluate(testFeature)
      }
    }

    this.indexSource = source.deepCopy(env)
  }

  override def hasTop = topKey.isDefined

  override def getTopKey = topKey.get

  override def getTopValue = topValue.get

  /**
   * Position the index-source.  Consequently, updates the data-source.
   *
   * @param range
   * @param columnFamilies
   * @param inclusive
   */
  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    // move the source iterator to the right starting spot
    indexSource.seek(range, columnFamilies, inclusive)
    // find the first index-entry that matches the filter
    findTop()
    // pre-fetch the next entry, if one exists (the idea is to always be one entry ahead)
    next()
  }

  /**
   * If there was a next, then we pre-fetched it, so we report those entries
   * back to the user, and make an attempt to pre-fetch another row, allowing
   * us to know whether there exists, in fact, a next entry.
   */
  override def next(): Unit = {
    topKey = nextKey
    topValue = nextValue
    if (nextKey.isDefined) {
      findTop()
    }
  }

  /**
   * Advances the index-iterator to the next qualifying entry
   */
  def findTop() {
    // clear out the reference to the next entry
    nextKey = None
    nextValue = None

    do {
      // increment the underlying iterator
      indexSource.next()

      // the value contains the full-resolution geometry and time
      val decodedValue = IndexEntry.decodeIndexValue(indexSource.getTopValue)

      if (filterTest(decodedValue.geom, decodedValue.dtgMillis)) {
        // current entry matches our filter - update the nextKey and nextValue
        // copy the key because reusing it is UNSAFE
        nextKey = Some(new Key(indexSource.getTopKey))
        // using the already decoded index value, generate a SimpleFeature
        val sf = IndexIterator.encodeIndexValueToSF(featureBuilder,
                                                    decodedValue.id,
                                                    decodedValue.geom,
                                                    decodedValue.dtgMillis)

        // TODO also encode the attribute from the row
        // set the encoded simple feature as the value
        nextValue = Some(new Value(featureEncoder.encode(sf)))
      } else {
        // increment the underlying iterator and loop again to the next entry
        indexSource.next()
      }
      // while there is more data and we haven't matched our filter
    } while (nextValue.isEmpty && indexSource.hasTop)
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("AttributeIndexIterator does not support deepCopy.")
}


