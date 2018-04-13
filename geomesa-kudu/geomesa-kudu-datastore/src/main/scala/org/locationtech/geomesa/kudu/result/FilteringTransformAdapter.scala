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
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.{FilterHelper, filterToString}
import org.locationtech.geomesa.kudu.result.KuduResultAdapter.KuduResultAdapterSerialization
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema
import org.locationtech.geomesa.security.{SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Expression, PropertyName}

case class FilteringTransformAdapter(sft: SimpleFeatureType,
                                     auths: Seq[Array[Byte]],
                                     ecql: Filter,
                                     tsft: SimpleFeatureType,
                                     tdefs: String) extends KuduResultAdapter {

  import scala.collection.JavaConverters._

  // determine all the attributes that we need to be able to evaluate the transform and filter
  private val attributes = {
    val fromTransform = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
      case p: PropertyName => Seq(p.getPropertyName)
      case e: Expression   => FilterHelper.propertyNames(e, sft)
    }
    val fromFilter = FilterHelper.propertyNames(ecql, sft)
    (fromTransform ++ fromFilter).distinct
  }

  private val subType = DataUtilities.createSubType(sft, attributes.toArray)
  subType.getUserData.putAll(sft.getUserData)

  private val schema = KuduSimpleFeatureSchema(sft)
  private val deserializer = schema.deserializer(subType)
  private val feature = new ScalaSimpleFeature(subType, "")
  private val transformFeature = TransformSimpleFeature(subType, tsft, tdefs)
  transformFeature.setFeature(feature)

  override val columns: Seq[String] =
    Seq(FeatureIdAdapter.name, VisibilityAdapter.name) ++ schema.schema(attributes).map(_.getName)

  override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    results.flatMap { row =>
      feature.setId(FeatureIdAdapter.readFromRow(row))
      deserializer.deserialize(row, feature)
      val vis = VisibilityAdapter.readFromRow(row)
      if ((vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)) && ecql.evaluate(feature)) {
        SecurityUtils.setFeatureVisibility(feature, vis)
        Iterator.single(transformFeature)
      } else {
        CloseableIterator.empty
      }
    }
  }

  override def toString: String =
    s"FilteringTransformAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"filter=${filterToString(ecql)}, transform=$tdefs, " +
        s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
}

object FilteringTransformAdapter extends KuduResultAdapterSerialization[FilteringTransformAdapter] {
  override def serialize(adapter: FilteringTransformAdapter, bb: ExpandingByteBuffer): Unit = {
    bb.putString(adapter.sft.getTypeName)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    bb.putInt(adapter.auths.length)
    adapter.auths.foreach(bb.putBytes)
    bb.putString(ECQL.toCQL(adapter.ecql))
    bb.putString(SimpleFeatureTypes.encodeType(adapter.tsft, includeUserData = true))
    bb.putString(adapter.tdefs)
  }

  override def deserialize(bb: ByteBuffer): FilteringTransformAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)
    val auths = (0 until bb.getInt).map(_ => bb.getBytes)
    val ecql = FastFilterFactory.toFilter(sft, bb.getString)
    val tsft = SimpleFeatureTypes.createType(sft.getTypeName, bb.getString)
    val tdefs = bb.getString
    FilteringTransformAdapter(sft, auths, ecql, tsft, tdefs)
  }
}