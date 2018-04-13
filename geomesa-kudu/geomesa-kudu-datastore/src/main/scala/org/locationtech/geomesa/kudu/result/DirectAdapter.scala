/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.result

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.kudu.client.RowResult
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kudu.result.KuduResultAdapter.KuduResultAdapterSerialization
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema
import org.locationtech.geomesa.security.{SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case class DirectAdapter(sft: SimpleFeatureType, auths: Seq[Array[Byte]]) extends KuduResultAdapter {

  private val schema = KuduSimpleFeatureSchema(sft)
  private val deserializer = schema.deserializer
  private val feature = new ScalaSimpleFeature(sft, "")

  override val columns: Seq[String] =
    Seq(FeatureIdAdapter.name, VisibilityAdapter.name) ++ schema.schema.map(_.getName)

  override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    results.flatMap { row =>
      feature.setId(FeatureIdAdapter.readFromRow(row))
      deserializer.deserialize(row, feature)
      val vis = VisibilityAdapter.readFromRow(row)
      if (vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)) {
        SecurityUtils.setFeatureVisibility(feature, vis)
        Iterator.single(feature)
      } else {
        CloseableIterator.empty
      }
    }
  }

  override def toString: String =
    s"DirectAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
}

object DirectAdapter extends KuduResultAdapterSerialization[DirectAdapter] {

  override def serialize(adapter: DirectAdapter, bb: ExpandingByteBuffer): Unit = {
    bb.putString(adapter.sft.getTypeName)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    bb.putInt(adapter.auths.length)
    adapter.auths.foreach(bb.putBytes)
  }

  override def deserialize(bb: ByteBuffer): DirectAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)
    val auths = (0 until bb.getInt).map(_ => bb.getBytes)

    DirectAdapter(sft, auths)
  }
}