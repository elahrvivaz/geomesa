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
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Configuration for GeoMesaAccumuloKryoInputFormat
  */
object GeoMesaAccumuloKryoInputFormat extends GeoMesaAccumuloInputFormatConfiguration

/**
  * Input format that returns kryo-serialized simple features
  */
class GeoMesaAccumuloKryoInputFormat extends AbstractGeoMesaAccumuloInputFormat[Array[Byte]] {

  override protected def createRecordReader(delegate: RecordReader[Key, Value],
                                            sft: SimpleFeatureType,
                                            index: AccumuloWritableIndex,
                                            config: Configuration): GeoMesaKryoRecordReader = {
    new GeoMesaKryoRecordReader(sft, index, delegate)
  }
}

/**
  * Returns raw serialized simple feature bytes
  *
  * @param sft simple feature type of serialized bytes
  * @param index index table being read
  * @param delegate delegate reader
  */
class GeoMesaKryoRecordReader(sft: SimpleFeatureType,
                              index: AccumuloWritableIndex,
                              delegate: RecordReader[Key, Value])
    extends AbstractGeoMesaAccumuloRecordReader[Array[Byte]](sft, index, delegate) {

  override protected def createNextValue(id: String, value: Array[Byte]): Array[Byte] = value
}