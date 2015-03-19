/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.io.serializer.{Deserializer, Serialization, Serializer}
import org.locationtech.geomesa.feature.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.jobs.mapreduce.SimpleFeatureSerialization._
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature

/**
 * Hadoop writable serialization for simple features
 */
class SimpleFeatureSerialization extends Serialization[SimpleFeature] {

  override def accept(c: Class[_]) = classOf[SimpleFeature].isAssignableFrom(c)

  override def getSerializer(c: Class[SimpleFeature]) = new SimpleFeatureSerializer

  override def getDeserializer(c: Class[SimpleFeature]) = new SimpleFeatureDeserializer
}

object SimpleFeatureSerialization {
  val serializers = new SoftThreadLocalCache[String, KryoFeatureSerializer]()
}

class SimpleFeatureSerializer extends Serializer[SimpleFeature] {

  var out: OutputStream = null

  override def open(out: OutputStream) = this.out = out

  override def close() = out.close()

  // TODO can we avoid serializing the sft each time?
  override def serialize(sf: SimpleFeature) = {
    val sft = sf.getFeatureType
    val sftString = SimpleFeatureTypes.encodeType(sft)
    val serializer = serializers.getOrElseUpdate(sftString, KryoFeatureSerializer(sft))
    val sftNameBytes = sft.getTypeName.getBytes("UTF-8")
    val sftStringBytes = sftString.getBytes("UTF-8")
    out.write(sftNameBytes.length)
    out.write(sftNameBytes)
    out.write(sftStringBytes.length)
    out.write(sftStringBytes)
    serializer.write(sf, out)
  }
}

class SimpleFeatureDeserializer extends Deserializer[SimpleFeature] {

  var in: InputStream = null

  override def open(in: InputStream) = this.in = in

  override def close() = in.close()

  override def deserialize(ignored: SimpleFeature) = {
    val sftNameBytes = Array.ofDim[Byte](in.read())
    in.read(sftNameBytes)
    val sftStringBytes = Array.ofDim[Byte](in.read())
    in.read(sftStringBytes)
    val sftString = new String(sftStringBytes, "UTF-8")
    lazy val sft = SimpleFeatureTypes.createType(new String(sftNameBytes, "UTF-8"), sftString)
    val serializer = serializers.getOrElseUpdate(sftString, KryoFeatureSerializer(sft))
    serializer.read(in)
  }
}

