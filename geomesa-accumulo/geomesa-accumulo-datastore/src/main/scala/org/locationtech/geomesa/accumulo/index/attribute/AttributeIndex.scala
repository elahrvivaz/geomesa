/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.attribute

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.{AccumuloFeatureIndex, AccumuloFilterStrategy}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object AttributeIndex extends AccumuloFeatureIndex with AccumuloWritableIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "attr"

  // 1 -> initial implementation
  // 2 -> added feature ID and dates to row key
  override val version: Int = 2

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.exists(_.isIndexed) && sft.getIndexVersion(name).exists(_ > 0)
  }

  override def getIndexVersion(schemaVersion: Int): Int = {
    // 6  -> attribute indices with dates
    if (schemaVersion < 6) { 1 } else { 2 }
  }

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) {
      AttributeIndexV6.writer(sft, ops)
    } else {
      AttributeIndexV5.writer(sft, ops)
    }

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) {
      AttributeIndexV6.remover(sft, ops)
    } else {
      AttributeIndexV5.remover(sft, ops)
    }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String =
    if (sft.getSchemaVersion > 5) {
      AttributeIndexV6.getIdFromRow(sft)
    } else {
      AttributeIndexV5.getIdFromRow(sft)
    }

  override def configure(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit =
    if (sft.getSchemaVersion > 5) {
      AttributeIndexV6.configure(sft, ops)
    } else {
      AttributeIndexV5.configure(sft, ops)
    }

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[AccumuloFilterStrategy] =
    // note: strategy is the same between versioned indices
    AttributeIndexV6.getFilterStrategy(sft, filter)

  override def getCost(sft: SimpleFeatureType,
                       ops: Option[AccumuloDataStore],
                       filter: AccumuloFilterStrategy,
                       transform: Option[SimpleFeatureType]): Long =
  // note: cost is the same between versioned indices
    AttributeIndexV6.getCost(sft, ops, filter, transform)

  override def getQueryPlan(sft: SimpleFeatureType,
                            ops: AccumuloDataStore,
                            filter: AccumuloFilterStrategy,
                            hints: Hints,
                            explain: Explainer): QueryPlan =
    if (sft.getSchemaVersion > 5) {
      AttributeIndexV6.getQueryPlan(sft, ops, filter, hints, explain)
    } else {
      AttributeIndexV5.getQueryPlan(sft, ops, filter, hints, explain)
    }

  object AttributeIndexV6 extends AccumuloFeatureIndex with AttributeWritableIndex with AttributeQueryableIndex {
    override def name: String = AttributeIndex.name
    override def version: Int = AttributeIndex.version
    override def supports(sft: SimpleFeatureType): Boolean = AttributeIndex.supports(sft)
    override def getIndexVersion(schemaVersion: Int): Int = AttributeIndex.getIndexVersion(schemaVersion)
  }

  // noinspection ScalaDeprecation
  object AttributeIndexV5 extends AccumuloFeatureIndex with AttributeWritableIndexV5 with AttributeQueryableIndexV5 {
    override def name: String = AttributeIndex.name
    override def version: Int = AttributeIndex.version
    override def supports(sft: SimpleFeatureType): Boolean = AttributeIndex.supports(sft)
    override def getIndexVersion(schemaVersion: Int): Int = AttributeIndex.getIndexVersion(schemaVersion)
  }
}