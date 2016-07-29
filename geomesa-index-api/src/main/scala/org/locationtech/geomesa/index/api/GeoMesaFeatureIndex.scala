/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.index.api

import org.geotools.data.DataStore
import org.geotools.factory.Hints
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait GeoMesaFeatureIndex[FeatureWrapper, Result, Row, Deleter, TableOps, Entries, Plan] {

  /**
    * The name used to identify the index
    */
  def name: String

  /**
    * Is the index compatible with the given feature type
    *
    * @param sft simple feature type
    * @return
    */
  def supports(sft: SimpleFeatureType): Boolean

  /**
    * Data operations
    *
    * @return
    */
  def writable: GeoMesaIndexWritable[FeatureWrapper, Result, Row, Deleter, TableOps, Entries]

  /**
    * Query operations
    *
    * @return
    */
  def queryable: GeoMesaIndexQueryable[Plan]

  /**
    * Trims off the $ of the object name
    *
    * @return
    */
  override def toString = getClass.getSimpleName.split("\\$").last
}

trait GeoMesaIndexWritable[FeatureWrapper, Result, Row, Deleter, TableOps, Entries] {

  /**
    * Creates a function to write a feature to the index
    */
  def writer(sft: SimpleFeatureType): (FeatureWrapper) => Seq[Result]

  /**
    * Creates a function to delete a feature to the index
    */
  def remover(sft: SimpleFeatureType): (FeatureWrapper) => Seq[Result]

  /**
    * Deletes all features from the table
    */
  def removeAll(sft: SimpleFeatureType, deleter: Deleter): Unit

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row
    */
  def getIdFromRow(sft: SimpleFeatureType): (Row) => String

  /**
    * Configure the underlying accumulo table
    *
    * @param sft      simple feature type
    * @param table    name of the accumulo table
    * @param tableOps handle to the accumulo table operations
    */
  def configure(sft: SimpleFeatureType, table: String, tableOps: TableOps): Unit

  /**
    * Transforms an iterator of Accumulo Key-Values into an iterator of SimpleFeatures
    *
    * @param sft simple feature type
    * @param returnSft simple feature type being returned (transform, aggregation, etc)
    * @return
    */
  def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Entries) => SimpleFeature
}

trait GeoMesaIndexQueryable[Plan] {

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    *   (bbox1 OR bbox2) AND dtg
    *   bbox AND dtg AND (attr1 = foo OR attr = bar)
    * not:
    *   bbox OR dtg
    *
    * Because the inputs are simple, each one can be satisfied with a single query filter.
    * The returned values will each satisfy the query.
    *
    * @param filter input filter
    * @return sequence of options, any of which can satisfy the query
    */
  def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[FilterStrategy]

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
  def getQueryPlan(ds: DataStore,
                   sft: SimpleFeatureType,
                   filter: FilterStrategy,
                   hints: Hints,
                   explain: Explainer = ExplainNull): Plan
}