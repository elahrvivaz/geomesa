/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Configuration for GeoMesaAccumuloMapInputFormat
  */
object GeoMesaAccumuloMapInputFormat extends GeoMesaAccumuloInputFormatConfiguration

/**
  * Input format that returns a map of attribute names to values
  */
class GeoMesaAccumuloMapInputFormat extends AbstractGeoMesaAccumuloInputFormat[Map[String, Any]] {

  override protected def createRecordReader(delegate: RecordReader[Key, Value],
                                            sft: SimpleFeatureType,
                                            index: AccumuloWritableIndex,
                                            config: Configuration): GeoMesaMapRecordReader = {
    import org.locationtech.geomesa.features.SerializationOption.SerializationOptions.{none, withoutId}
    val schema = GeoMesaConfigurator.getTransformSchema(config).getOrElse(sft)
    val serializationOptions = if (index.serializedWithId) { none } else { withoutId }
    val serializer = new KryoFeatureSerializer(schema, serializationOptions)
    new GeoMesaMapRecordReader(sft, index, delegate, serializer)
  }
}

/**
  * Transforms serialized simple feature into map of attributes to values
  *
  * @param sft simple feature type of serialized bytes
  * @param index index table being read
  * @param delegate delegate reader
  * @param serializer simple feature serializer for the data being read
  */
class GeoMesaMapRecordReader(sft: SimpleFeatureType,
                             index: AccumuloWritableIndex,
                             delegate: RecordReader[Key, Value],
                             serializer: KryoFeatureSerializer)
    extends AbstractGeoMesaAccumuloRecordReader[Map[String, Any]](sft, index, delegate) {

  import scala.collection.JavaConversions._

  private var mapKeys: Map[String, Int] = null

  override protected def createNextValue(id: String, value: Array[Byte]): Map[String, Any] = {
    val sf = serializer.deserialize(value)
    if (mapKeys == null) {
      mapKeys = sf.getFeatureType.getAttributeDescriptors.map(_.getLocalName).zipWithIndex.toMap
    }
    mapKeys.mapValues(sf.getAttribute) + (("id", sf.getID))
  }

  override def close(): Unit = {
    mapKeys = null
    super.close()
  }
}