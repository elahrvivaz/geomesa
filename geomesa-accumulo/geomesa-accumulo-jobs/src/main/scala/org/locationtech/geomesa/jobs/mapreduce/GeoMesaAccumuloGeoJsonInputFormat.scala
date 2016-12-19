/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{BufferedWriter, StringWriter}

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Configuration for GeoMesaAccumuloGeoJsonInputFormat
  */
object GeoMesaAccumuloGeoJsonInputFormat extends GeoMesaAccumuloInputFormatConfiguration

/**
  * Input format that returns a geojson string
  */
class GeoMesaAccumuloGeoJsonInputFormat extends AbstractGeoMesaAccumuloInputFormat[String] {

  override protected def createRecordReader(delegate: RecordReader[Key, Value],
                                            sft: SimpleFeatureType,
                                            index: AccumuloWritableIndex,
                                            config: Configuration): GeoMesaGeoJsonRecordReader = {
    import org.locationtech.geomesa.features.SerializationOption.SerializationOptions.{none, withoutId}
    val schema = GeoMesaConfigurator.getTransformSchema(config).getOrElse(sft)
    val serializationOptions = if (index.serializedWithId) { none } else { withoutId }
    val serializer = new KryoFeatureSerializer(schema, serializationOptions)
    new GeoMesaGeoJsonRecordReader(sft, index, delegate, serializer)
  }
}

/**
  * Transforms serialized simple feature into GeoJson
  *
  * @param sft simple feature type of serialized bytes
  * @param index index table being read
  * @param delegate delegate reader
  * @param serializer simple feature serializer for the data being read
  */
class GeoMesaGeoJsonRecordReader(sft: SimpleFeatureType,
                             index: AccumuloWritableIndex,
                             delegate: RecordReader[Key, Value],
                             serializer: KryoFeatureSerializer)
    extends AbstractGeoMesaAccumuloRecordReader[String](sft, index, delegate) {

  private val json = new FeatureJSON
  private val stringWriter = new StringWriter
  // FeatureJSON will create a BufferedWriter if we don't pass one in
  private val bufferedWriter = new BufferedWriter(stringWriter)

  override protected def createNextValue(id: String, value: Array[Byte]): String = {
    try {
      json.writeFeature(serializer.deserialize(value), bufferedWriter)
      bufferedWriter.flush()
      stringWriter.toString
    } finally {
      stringWriter.getBuffer.setLength(0) // clear the underlying buffer
    }
  }
}