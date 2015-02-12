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

package org.locationtech.geomesa.core.index

import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.core
import org.locationtech.geomesa.feature.{KryoFeatureEncoder, ScalaSimpleFeature}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable

/**
 * Encoder/decoder for index values. Allows customizable fields to be encoded. Not thread-safe.
 *
 * @param sft
 * @param indices
 * @param encoder
 */
class IndexValueEncoder(sft: SimpleFeatureType, indices: Array[Int], encoder: KryoFeatureEncoder) {

  import scala.collection.JavaConversions._

  /**
   * Encodes a simple feature into a byte array. Only the attributes marked for inclusion get encoded.
   *
   * @param sf
   * @return
   */
  def encode(sf: SimpleFeature): Array[Byte] = {
    val attributes = indices.map(sf.getAttribute)
    val toEncode = new ScalaSimpleFeature(sf.getID, sft, attributes)
    encoder.encode(toEncode)
  }

  /**
   * Decodes a byte array into a map of attribute name -> attribute value pairs
   *
   * @param value
   * @return
   */
  def decode(value: Array[Byte]): SimpleFeature = encoder.decode(value)

  def fields(): Seq[String] = sft.getAttributeDescriptors.map(_.getLocalName)
}

object IndexValueEncoder {

  import scala.collection.JavaConversions._

  // gets a cached instance to avoid the initialization overhead
  // we use sft.toString, which includes the fields and type name, as a unique key
  def apply(sft: SimpleFeatureType) = {
    val geom = sft.getGeometryDescriptor
    val dtg = core.index.getDtgFieldName(sft)
    val attributes = mutable.Buffer.empty[AttributeDescriptor]
    val indices = mutable.Buffer.empty[Int]
    var i = 0
    while (i < sft.getAttributeCount) {
      val ad = sft.getDescriptor(i)
      if (ad == geom || dtg.exists(_ == ad.getLocalName) ||
          Option(ad.getUserData.get(OPT_INDEX_VALUE).asInstanceOf[Boolean]).getOrElse(false)) {
        attributes.append(ad)
        indices.append(i)
      }
      i += 1
    }
    val builder = new SimpleFeatureTypeBuilder()
    builder.setName(sft.getTypeName + "--index")
    builder.setAttributes(attributes)
    builder.setDefaultGeometry(geom.getLocalName)
    builder.setCRS(sft.getCoordinateReferenceSystem)
    val indexSft = builder.buildFeatureType()
    indexSft.getUserData.putAll(sft.getUserData)
    new IndexValueEncoder(indexSft, indices.toArray, new KryoFeatureEncoder(indexSft, indexSft))
  }
}