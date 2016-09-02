/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client.{Mutation, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseWritableFeature}
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.HBaseFeatureIndexType
import org.locationtech.geomesa.index.api.{FilterPlan, FilterStrategy, GeoMesaFeatureIndex}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object HBaseFeatureIndex {

  type HBaseFeatureIndexType = GeoMesaFeatureIndex[HBaseDataStore, HBaseWritableFeature, Mutation, HBaseQueryPlan]
  type HBaseFilterPlan = FilterPlan[HBaseDataStore, HBaseWritableFeature, Mutation, HBaseQueryPlan]
  type HBaseFilterStrategy = FilterStrategy[HBaseDataStore, HBaseWritableFeature, Mutation, HBaseQueryPlan]

  val AllIndices: Seq[HBaseFeatureIndex] = Seq(HBaseZ3Index)

  val DataColumnFamily = Bytes.toBytes("D")
  val DataColumnFamilyDescriptor = new HColumnDescriptor(DataColumnFamily)

  val EmptyBytes = Array.empty[Byte]

  val DefaultNumSplits = 4 // can't be more than Byte.MaxValue (127)
  val DefaultSplitArrays = (0 until DefaultNumSplits).map(_.toByte).toArray.map(Array(_)).toSeq


}

trait HBaseFeatureIndex extends HBaseFeatureIndexType {


  def getTableName(sft: SimpleFeatureType): TableName = TableName.valueOf(s"${sft.getTypeName}_$name")

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row
    */
  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String

  /**
    * Turns accumulo results into simple features
    *
    * @param sft simple feature type
    * @param returnSft return simple feature type (transform, etc)
    * @return
    */
  def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Result) => SimpleFeature = {
    // Perform a projecting decode of the simple feature
    val getId = getIdFromRow(sft)
    val deserializer = new KryoFeatureSerializer(returnSft, SerializationOptions.withoutId)
    (result) => {
      val entries = result.getFamilyMap(HBaseFeatureIndex.DataColumnFamily)
      val sf = deserializer.deserialize(entries.values.iterator.next)
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(result.getRow))
      sf
    }
  }
}