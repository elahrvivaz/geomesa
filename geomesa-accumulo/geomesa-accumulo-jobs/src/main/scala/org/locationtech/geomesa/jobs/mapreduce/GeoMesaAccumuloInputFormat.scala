/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.net.{URL, URLClassLoader}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, InputFormatBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

object GeoMesaAccumuloInputFormat extends GeoMesaAccumuloInputFormatObject

/**
 * Input format that allows processing of simple features from GeoMesa based on a CQL query
 */
class GeoMesaAccumuloInputFormat extends AbstractGeoMesaAccumuloInputFormat[SimpleFeature] {

  override protected def createRecordReader(delegate: RecordReader[Key, Value],
                                            sft: SimpleFeatureType,
                                            index: AccumuloWritableIndex,
                                            config: Configuration): RecordReader[Text, SimpleFeature] = {
    val schema = GeoMesaConfigurator.getTransformSchema(config).getOrElse(sft)
    val hasId = index.serializedWithId
    val serializationOptions = if (hasId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    val decoder = new KryoFeatureSerializer(schema, serializationOptions)
    new GeoMesaRecordReader(sft, index, delegate, hasId, decoder)
  }
}

/**
 * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
 * simple features.
 *
 * @param reader delegate accumulo record reader
 */
class GeoMesaRecordReader(sft: SimpleFeatureType,
                          table: AccumuloWritableIndex,
                          reader: RecordReader[Key, Value],
                          hasId: Boolean,
                          decoder: SimpleFeatureSerializer) extends RecordReader[Text, SimpleFeature] {

  private var currentFeature: SimpleFeature = null

  private val getId = table.getIdFromRow(sft)

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
    reader.initialize(split, context)

  override def getProgress: Float = reader.getProgress

  override def nextKeyValue(): Boolean = {
    if (!reader.nextKeyValue()) { false } else {
      currentFeature = decoder.deserialize(reader.getCurrentValue.get())
      if (!hasId) {
        val row = reader.getCurrentKey.getRow
        currentFeature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength))
      }
      true
    }
  }

  override def getCurrentValue: SimpleFeature = currentFeature

  override def getCurrentKey: Text = new Text(currentFeature.getID)

  override def close(): Unit = reader.close()
}
