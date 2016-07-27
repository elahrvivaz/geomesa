/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.index.id.{RecordIdxStrategy, RecordTable}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializers}
import org.locationtech.geomesa.security.SecurityUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait AccumuloFeatureIndex extends AccumuloMutableIndex with AccumuloQueryableIndex {

  /**
    * The name used to identify the index
    */
  def name: String

  /**
    * Is the index compatible with the given feature type
    */
  def supports(sft: SimpleFeatureType): Boolean
}

trait AccumuloMutableIndex {

  /**
    * Creates a function to write a feature to the index
    */
  def writer(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation]

  /**
    * Creates a function to delete a feature to the index
    */
  def remover(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation]

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row
    */
  def getIdFromRow(sft: SimpleFeatureType): (Text) => String

  def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit

  /**
    * Deletes all features from the table
    */
  def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    import org.apache.accumulo.core.data.{Range => aRange}
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._
    val prefix = new Text(sft.getTableSharingPrefix)
    bd.setRanges(Seq(new aRange(prefix, true, aRange.followingPrefix(prefix), false)))
    bd.delete()
  }
}

trait AccumuloQueryableIndex {

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    *   (bbox1 OR bbox2) AND dtg
    *   bbox AND dtg AND (attr1 = foo OR attr = bar)
    * not:
    *   bbox OR dtg
    *
    * Because the inputs are simple, each one can be satisfied with a single query filter.
    * The returned values will satisfy the query.
    *
    * @param filter input filter
    * @return sequence of options, any of which can satisfy the query
    */
  def getSimpleQueryFilter(sft: SimpleFeatureType, filter: Filter): Seq[FilterStrategy]

  /**
    * Gets the estimated cost of running the query. In general, this is the estimated
    * number of features that will have to be scanned.
    */
  def getCost(sft: SimpleFeatureType,
              stats: Option[GeoMesaStats],
              filter: FilterStrategy,
              transform: Option[SimpleFeatureType]): Long

  /**
    * Plans the query - strategy implementations need to define this
    */
  def getQueryPlan(ds: AccumuloDataStore,
                   sft: SimpleFeatureType,
                   filter: FilterStrategy,
                   hints: Hints,
                   explain: ExplainerOutputType = ExplainNull): QueryPlan
}

object AccumuloQueryableIndex {

  // This function decodes/transforms that Iterator of Accumulo Key-Values into an Iterator of SimpleFeatures
  def kvsToFeatures(sft: SimpleFeatureType,
                    returnSft: SimpleFeatureType,
                    index: AccumuloMutableIndex): (java.util.Map.Entry[Key, Value]) => SimpleFeature = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // Perform a projecting decode of the simple feature
    if (sft.getSchemaVersion < 9) {
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        applyVisibility(sf, kv.getKey)
        sf
      }
    } else {
      val getId = index.getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO, SerializationOptions.withoutId)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(kv.getKey.getRow))
        applyVisibility(sf, kv.getKey)
        sf
      }
    }
  }

  def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
    val visibility = key.getColumnVisibility
    if (visibility.getLength > 0) {
      SecurityUtils.setFeatureVisibility(sf, visibility.toString)
    }
  }
}

abstract class DelegatingFeatureIndex(mutable: AccumuloMutableIndex, queryable: AccumuloQueryableIndex)
    extends AccumuloFeatureIndex {

  override def writer(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] = mutable.writer(sft)

  override def remover(sft: SimpleFeatureType): (WritableFeature) => Seq[Mutation] = mutable.remover(sft)

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = mutable.getIdFromRow(sft)

  override def configureTable(sft: SimpleFeatureType,
                              table: String,
                              tableOps: TableOperations): Unit = mutable.configureTable(sft, table, tableOps)

  override def getSimpleQueryFilter(sft: SimpleFeatureType,
                                    filter: Filter): Seq[FilterStrategy] =
    queryable.getSimpleQueryFilter(sft, filter)

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