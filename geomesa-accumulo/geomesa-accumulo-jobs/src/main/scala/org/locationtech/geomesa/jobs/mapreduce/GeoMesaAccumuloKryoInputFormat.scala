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
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Configuration for GeoMesaAccumuloKryoInputFormat
  */
object GeoMesaAccumuloKryoInputFormat extends GeoMesaAccumuloInputFormatObject

/**
  * Input format that returns kryo-serialized simple features
  */
class GeoMesaAccumuloKryoInputFormat extends AbstractGeoMesaAccumuloInputFormat[Array[Byte]] {

  override protected def createRecordReader(delegate: RecordReader[Key, Value],
                                            sft: SimpleFeatureType,
                                            index: AccumuloWritableIndex,
                                            config: Configuration): RecordReader[Text, Array[Byte]] = {
    new GeoMesaKryoRecordReader(sft, index, delegate)
  }
}

/**
  * Record reader that delegates to accumulo record readers
  *
  * @param reader delegate accumulo record reader
  */
class GeoMesaKryoRecordReader(sft: SimpleFeatureType,
                              table: AccumuloWritableIndex,
                              reader: RecordReader[Key, Value]) extends RecordReader[Text, Array[Byte]] {

  private val getId = table.getIdFromRow(sft)

  private var currentKey: Text = null
  private var currentValue: Array[Byte] = null

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = reader.initialize(split, context)

  override def getProgress: Float = reader.getProgress

  override def nextKeyValue(): Boolean = {
    if (!reader.nextKeyValue()) { false } else {
      val row = reader.getCurrentKey.getRow
      currentKey = new Text(getId(row.getBytes, 0, row.getLength))
      currentValue = reader.getCurrentValue.get()
      true
    }
  }

  override def getCurrentValue: Array[Byte] = currentValue

  override def getCurrentKey: Text = currentKey

  override def close(): Unit = reader.close()
}
