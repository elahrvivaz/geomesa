/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index.attribute

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFilterStrategy
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object AttributeIndex extends AccumuloFeatureIndex {

  override val name: String = "attr"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.exists(_.isIndexed) &&
        (sft.getEnabledTables.isEmpty || Seq(name, "attr_idx").exists(sft.getEnabledTables.contains)) // check for old suffix
  }

  override val writable: AccumuloWritableIndex = AttributeMergedWritableIndex
  override val queryable: AccumuloQueryableIndex = AttributeMergedQueryableIndex
}

object AttributeMergedWritableIndex extends AccumuloWritableIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val index: AccumuloFeatureIndex = AttributeIndex

  override def writer(sft: SimpleFeatureType, table: String): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) {
      AttributeWritableIndex.writer(sft, table)
    } else {
      AttributeWritableIndexV5.writer(sft, table)
    }

  override def remover(sft: SimpleFeatureType, table: String): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) {
      AttributeWritableIndex.remover(sft, table)
    } else {
      AttributeWritableIndexV5.remover(sft, table)
    }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String =
    if (sft.getSchemaVersion > 5) {
      AttributeWritableIndex.getIdFromRow(sft)
    } else {
      AttributeWritableIndexV5.getIdFromRow(sft)
    }

  override def configure(sft: SimpleFeatureType, ops: AccumuloDataStore, table: String): Unit =
    if (sft.getSchemaVersion > 5) {
      AttributeWritableIndex.configure(sft, ops, table)
    } else {
      AttributeWritableIndexV5.configure(sft, ops, table)
    }
}


object AttributeMergedQueryableIndex extends AccumuloQueryableIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val index: AccumuloFeatureIndex = AttributeIndex

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[AccumuloFilterStrategy] =
    AttributeQueryableIndex.getFilterStrategy(sft, filter)

  override def getCost(sft: SimpleFeatureType,
                       ops: Option[AccumuloDataStore],
                       filter: AccumuloFilterStrategy,
                       transform: Option[SimpleFeatureType]): Long =
    AttributeQueryableIndex.getCost(sft, ops, filter, transform)

  override def getQueryPlan(sft: SimpleFeatureType,
                            ops: AccumuloDataStore,
                            filter: AccumuloFilterStrategy,
                            hints: Hints,
                            explain: Explainer): QueryPlan =
    if (sft.getSchemaVersion > 5) {
      AttributeQueryableIndex.getQueryPlan(sft, ops, filter, hints, explain)
    } else {
      AttributeQueryableIndexV5.getQueryPlan(sft, ops, filter, hints, explain)
    }

}