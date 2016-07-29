/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializers}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.security.SecurityUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait AccumuloFeatureIndex extends
    GeoMesaFeatureIndex[AccumuloDataStore, WritableFeature, Mutation, Text, Entry[Key, Value], QueryPlan]

trait AccumuloIndexWritable extends
    GeoMesaIndexWritable[AccumuloDataStore, WritableFeature, Mutation, Text, Entry[Key, Value], QueryPlan] {

  override def removeAll(sft: SimpleFeatureType, ops: AccumuloDataStore, table: String): Unit = {
    import org.apache.accumulo.core.data.{Range => aRange}
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    val auths = ops.authProvider.getAuthorizations
    val config = GeoMesaBatchWriterConfig().setMaxWriteThreads(ops.config.writeThreads)
    val prefix = new Text(sft.getTableSharingPrefix)
    val deleter = ops.connector.createBatchDeleter(table, auths, ops.config.queryThreads, config)
    try {
      deleter.setRanges(Seq(new aRange(prefix, true, aRange.followingPrefix(prefix), false)))
      deleter.delete()
    } finally {
      deleter.close()
    }
  }

  override def entriesToFeatures(sft: SimpleFeatureType,
                                 returnSft: SimpleFeatureType): (Entry[Key, Value]) => SimpleFeature = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // Perform a projecting decode of the simple feature
    if (sft.getSchemaVersion < 9) {
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        AccumuloFeatureIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    } else {
      val getId = getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO, SerializationOptions.withoutId)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(kv.getKey.getRow))
        AccumuloFeatureIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    }
  }
}

trait AccumuloIndexQueryable extends
    GeoMesaIndexQueryable[AccumuloDataStore, WritableFeature, Mutation, Text, Entry[Key, Value], QueryPlan]

object AccumuloFeatureIndex {

  type AccumuloFilterPlan = FilterPlan[AccumuloDataStore, WritableFeature, Mutation, Text, Entry[Key, Value], QueryPlan]
  type AccumuloFilterStrategy = FilterStrategy[AccumuloDataStore, WritableFeature, Mutation, Text, Entry[Key, Value], QueryPlan]

  val FullColumnFamily      = new Text("F")
  val IndexColumnFamily     = new Text("I")
  val BinColumnFamily       = new Text("B")
  val AttributeColumnFamily = new Text("A")

  val EmptyColumnQualifier  = new Text()

  val NullByte = Array(0.toByte)

  def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
    val visibility = key.getColumnVisibility
    if (visibility.getLength > 0) {
      SecurityUtils.setFeatureVisibility(sf, visibility.toString)
    }
  }
}
