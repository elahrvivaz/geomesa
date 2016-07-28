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

  override val name: String = "attr"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.exists(_.isIndexed) &&
        (sft.getEnabledTables.isEmpty || Seq(name, "attr_idx").exists(sft.getEnabledTables.contains)) // check for old suffix
  }

  override val writable: AccumuloIndexWritable = AttributeMergedIndexWritable
  override val queryable: AccumuloIndexQueryable = AttributeMergedIndexQueryable
}

object AttributeMergedIndexWritable extends AccumuloIndexWritable {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override def writer(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) AttributeIndexWritable.writer(sft) else AttributeIndexWritableV5.writer(sft)

  override def remover(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] =
    if (sft.getSchemaVersion > 5) AttributeIndexWritable.remover(sft) else AttributeIndexWritableV5.remover(sft)

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String =
    if (sft.getSchemaVersion > 5) AttributeIndexWritable.getIdFromRow(sft) else AttributeIndexWritableV5.getIdFromRow(sft)

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit =
    if (sft.getSchemaVersion > 5) {
      AttributeIndexWritable.configureTable(sft, table, tableOps)
    } else {
      AttributeIndexWritableV5.configureTable(sft, table, tableOps)
    }
}


object AttributeMergedIndexQueryable extends AccumuloIndexQueryable {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[FilterStrategy] =
    AttributeIndexQueryable.getFilterStrategy(sft, filter)

  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: FilterStrategy,
                       transform: Option[SimpleFeatureType]): Long =
    AttributeIndexQueryable.getCost(sft, stats, filter, transform)

  override def getQueryPlan(ds: AccumuloDataStore,
                            sft: SimpleFeatureType,
                            filter: FilterStrategy,
                            hints: Hints,
                            explain: ExplainerOutputType): QueryPlan =
    if (sft.getSchemaVersion > 5) {
      AttributeIndexQueryable.getQueryPlan(ds, sft, filter, hints, explain)
    } else {
      AttributeIndexQueryableV5.getQueryPlan(ds, sft, filter, hints, explain)
    }

}