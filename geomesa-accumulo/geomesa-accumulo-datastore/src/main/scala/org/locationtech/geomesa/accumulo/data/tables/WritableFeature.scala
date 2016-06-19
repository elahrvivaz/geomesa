/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.tables

import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.{BinEncoder, IndexValueEncoder}
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.hashing.MurmurHash3

trait WritableFeature {
  def feature: SimpleFeature
  def values: Seq[RowValue]
  def indexValues: Seq[RowValue]
  def binValues: Seq[RowValue]
  def idHash: Int
}

object WritableFeature {

  def apply(feature: SimpleFeature,
            sft: SimpleFeatureType,
            defaultVisibility: String,
            serializer: SimpleFeatureSerializer,
            indexEncoder: IndexValueEncoder,
            binEncoder: Option[BinEncoder]): WritableFeature = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   =>
        new WriteableFeatureLevelFeature(feature, sft, defaultVisibility, serializer, indexEncoder, binEncoder)
      case VisibilityLevel.Attribute =>
        new WriteableAttributeLevelFeature(feature, sft, defaultVisibility, serializer, indexEncoder, binEncoder)
    }
  }
}

case class RowValue(cf: Text, cq: Text, vis: ColumnVisibility, value: Value)

class WriteableFeatureLevelFeature(val feature: SimpleFeature,
                                   sft: SimpleFeatureType,
                                   defaultVisibility: String,
                                   serializer: SimpleFeatureSerializer,
                                   indexValueEncoder: IndexValueEncoder,
                                   binEncoder: Option[BinEncoder]) extends WritableFeature {
  // hash value of the feature id
  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))
}

class WriteableAttributeLevelFeature(val feature: SimpleFeature,
                                     sft: SimpleFeatureType,
                                     defaultVisibility: String,
                                     serializer: SimpleFeatureSerializer,
                                     indexValueEncoder: IndexValueEncoder,
                                     binEncoder: Option[BinEncoder]) extends WritableFeature {
  // hash value of the feature id
  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))
}

class FeatureToWrite(val feature: SimpleFeature,
                     sft: SimpleFeatureType,
                     defaultVisibility: String,
                     serializer: SimpleFeatureSerializer,
                     indexValueEncoder: IndexValueEncoder,
                     binEncoder: Option[BinEncoder]) {

  import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
//
  lazy val rowValues: Seq[RowValue] = {
  if (sft)
  }

  lazy val visibility = new Text(feature.userData[String](FEATURE_VISIBILITY).getOrElse(defaultVisibility))

  lazy val columnVisibility = new ColumnVisibility(visibility)
    // the index value is the encoded date/time/fid
  lazy val indexValue = new Value(indexValueEncoder.encode(feature))
    // the data value is the encoded SimpleFeature
  lazy val dataValue = new Value(serializer.serialize(feature))
    // bin formatted value
  lazy val binValue = binEncoder.map(e => new Value(e.encode(feature)))


    // TODO optimize for case where all vis are the same
  lazy val perAttributeValues: Seq[RowValue] = {
  val count = feature.getFeatureType.getAttributeCount
  val visibilities = feature.userData[String](FEATURE_VISIBILITY).map(_.split(","))
  .getOrElse(Array.fill(count)(defaultVisibility))
  require(visibilities.length == count, "Per-attribute visibilities do not match feature type")
  val groups = visibilities.zipWithIndex.groupBy(_._1).mapValues(_.map(_._2.toByte).sorted).toSeq
  groups.map { case (vis, indices) =>
  val cq = new Text(indices)
  val values = indices.map(i => serializer.serialize(i, feature.getAttribute(i)))
  val output = KryoFeatureSerializer.getOutput() // note: same output object used in serializer.serialize
  values.foreach { value =>
  output.writeInt(value.length, true)
  output.write(value)
  }
  val value = new Value(output.toBytes)
  RowValue(GeoMesaTable.PerAttributeColumnFamily, cq, new ColumnVisibility(vis), value)
  }
  }
  }