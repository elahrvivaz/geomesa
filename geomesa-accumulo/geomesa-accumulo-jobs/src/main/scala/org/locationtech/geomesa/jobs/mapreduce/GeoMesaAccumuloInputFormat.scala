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
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object GeoMesaAccumuloInputFormat extends GeoMesaAccumuloInputFormatConfiguration

/**
 * Input format that allows processing of simple features from GeoMesa based on a CQL query
 */
class GeoMesaAccumuloInputFormat extends AbstractGeoMesaAccumuloInputFormat[SimpleFeature] {

  override protected def createRecordReader(delegate: RecordReader[Key, Value],
                                            sft: SimpleFeatureType,
                                            index: AccumuloWritableIndex,
                                            config: Configuration): GeoMesaAccumuloRecordReader = {
    val schema = GeoMesaConfigurator.getTransformSchema(config).getOrElse(sft)
    val hasId = index.serializedWithId
    val serializationOptions = if (hasId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    val serializer = new KryoFeatureSerializer(schema, serializationOptions)
    new GeoMesaAccumuloRecordReader(sft, index, delegate, hasId, serializer)
  }
}

/**
  * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
  * simple features
  *
  * @param sft simple feature type
  * @param index index table being read
  * @param delegate delegate record reader
  * @param hasId feature is serialized with ID or not
  * @param serializer simple feature serializer
  */
class GeoMesaAccumuloRecordReader(sft: SimpleFeatureType,
                                  index: AccumuloWritableIndex,
                                  delegate: RecordReader[Key, Value],
                                  hasId: Boolean,
                                  serializer: SimpleFeatureSerializer)
    extends AbstractGeoMesaAccumuloRecordReader[SimpleFeature](sft, index, delegate) {

  override protected def createNextValue(id: String, value: Array[Byte]): SimpleFeature = {
    val sf = serializer.deserialize(value)
    if (!hasId) {
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
    }
    sf
  }
}
