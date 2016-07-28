/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Helper class to transition from old table/strategy implementations
  *
  * @param mutable table class
  * @param queryable strategy class
  */
abstract class AccumuloSplitIndex(mutable: MutableFeatureIndex, queryable: QueryableFeatureIndex)
    extends AccumuloFeatureIndex {

  override def writer(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] = mutable.writer(sft)

  override def remover(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] = mutable.remover(sft)

  override def removeAll(sft: SimpleFeatureType, bd: BatchDeleter): Unit = mutable.removeAll(sft, bd)

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = mutable.getIdFromRow(sft)

  override def configureTable(sft: SimpleFeatureType,
                              table: String,
                              tableOps: TableOperations): Unit = mutable.configureTable(sft, table, tableOps)

  override def getFilterStrategy(sft: SimpleFeatureType,
                                 filter: Filter): Seq[FilterStrategy] =
    queryable.getFilterStrategy(sft, filter)

  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: FilterStrategy,
                       transform: Option[SimpleFeatureType]): Long =
    queryable.getCost(sft, stats, filter, transform)

  override def getQueryPlan(ds: AccumuloDataStore,
                            sft: SimpleFeatureType,
                            filter: FilterStrategy,
                            hints: Hints,
                            explain: ExplainerOutputType): QueryPlan =
    queryable.getQueryPlan(ds, sft, filter, hints, explain)
}

/**
  * Helper for transitioning old table classes
  */
abstract class MutableFeatureIndex extends AccumuloFeatureIndex {

  override def name: String = throw new NotImplementedError

  override def supports(sft: SimpleFeatureType): Boolean = throw new NotImplementedError

  override def getFilterStrategy(sft: SimpleFeatureType,
                                 filter: Filter): Seq[FilterStrategy] = throw new NotImplementedError

  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: FilterStrategy,
                       transform: Option[SimpleFeatureType]): Long = throw new NotImplementedError

  override def getQueryPlan(ds: AccumuloDataStore,
                            sft: SimpleFeatureType,
                            filter: FilterStrategy,
                            hints: Hints,
                            explain: ExplainerOutputType): QueryPlan = throw new NotImplementedError
}

/**
  * Helper for transitioning old strategy classes
  */
abstract class QueryableFeatureIndex extends AccumuloFeatureIndex {

  override def name: String = throw new NotImplementedError

  override def supports(sft: SimpleFeatureType): Boolean = throw new NotImplementedError

  override def writer(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] = throw new NotImplementedError

  override def remover(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] = throw new NotImplementedError

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = throw new NotImplementedError

  override def configureTable(sft: SimpleFeatureType,
                              table: String,
                              tableOps: TableOperations): Unit = throw new NotImplementedError
}
