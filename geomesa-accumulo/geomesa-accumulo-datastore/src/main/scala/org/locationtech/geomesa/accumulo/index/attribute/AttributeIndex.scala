/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index.attribute

import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object AttributeIndex extends AccumuloFeatureIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "attr"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConversions._
    sft.getSchemaVersion > 5 && sft.getAttributeDescriptors.exists(_.isIndexed) &&
        (sft.getEnabledTables.isEmpty || sft.getEnabledTables.contains(name) || sft.getEnabledTables.contains("attr_idx")) // check for old suffix
  }

  object V6Index extends AccumuloSplitIndex(AttributeTable, AttributeIdxStrategy) {
    override val name: String = null
    override def supports(sft: SimpleFeatureType): Boolean = false
  }

  @deprecated
  object V5Index extends AccumuloSplitIndex(AttributeTableV5, AttributeIdxStrategyV5) {
    override val name: String = "attr"
    override def supports(sft: SimpleFeatureType): Boolean = false
  }

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[FilterStrategy] =
    V6Index.getFilterStrategy(sft, filter)

  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: FilterStrategy,
                       transform: Option[SimpleFeatureType]): Long = V6Index.getCost(sft, stats, filter, transform)

  override def getQueryPlan(ds: AccumuloDataStore,
                            sft: SimpleFeatureType,
                            filter: FilterStrategy,
                            hints: Hints,
                            explain: ExplainerOutputType): QueryPlan =
    if (sft.getSchemaVersion > 5) {
      V6Index.getQueryPlan(ds, sft, filter, hints, explain)
    } else {
      V5Index.getQueryPlan(ds, sft, filter, hints, explain)
    }

  override def writer(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) V6Index.writer(sft) else V5Index.writer(sft)

  override def remover(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) V6Index.remover(sft) else V5Index.remover(sft)

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String =
    if (sft.getSchemaVersion > 5) V6Index.getIdFromRow(sft) else V5Index.getIdFromRow(sft)

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit =
    if (sft.getSchemaVersion > 5) {
      V6Index.configureTable(sft, table, tableOps)
    } else {
      V5Index.configureTable(sft, table, tableOps)
    }

  override val toString = getClass.getSimpleName.split("\\$").last
}
