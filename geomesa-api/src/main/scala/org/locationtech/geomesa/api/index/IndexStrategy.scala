/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.api.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.api.stats.GeoMesaStats
import org.locationtech.geomesa.api.utils.{ExplainNull, ExplainQuery}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait IndexStrategy[WriteKeyValue, WriteResult, RowKey, QueryResult] {

  /**
    * The name used to identify the index
    */
  def name: String

  /**
    * Is the index compatible with the given feature type
    */
  def supports(sft: SimpleFeatureType): Boolean

  def mutable: MutableIndex[WriteKeyValue, WriteResult, RowKey]

  def queryable: QueryableIndex[QueryResult]
}

trait MutableIndex[WriteKeyValue, WriteResult, RowKey] {

  /**
    * Creates a function to write a feature to the index
    */
  def writer(sft: SimpleFeatureType): (WritableFeature[WriteKeyValue]) => Seq[WriteResult]

  /**
    * Creates a function to delete a feature to the index
    */
  def remover(sft: SimpleFeatureType): (WritableFeature[WriteKeyValue]) => Seq[WriteResult]

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row
    */
  def getIdFromRow(sft: SimpleFeatureType): (RowKey) => String
}

trait QueryableIndex[QueryResult] {

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
  def getSimpleQueryFilter(filter: Filter): Option[QueryFilter[QueryResult]]

  /**
    * Gets the estimated cost of running the query. In general, this is the estimated
    * number of features that will have to be scanned.
    */
  def getCost(sft: SimpleFeatureType,
              stats: Option[GeoMesaStats],
              filter: QueryFilter[QueryResult],
              transform: Option[SimpleFeatureType]): Long

  /**
    * Plans the query - strategy implementations need to define this
    */
  def getQueryPlan(sft: SimpleFeatureType,
                   filter: QueryFilter[QueryResult],
                   hints: Hints,
                   explain: ExplainQuery = ExplainNull): QueryPlan[QueryResult]
}

object IndexStrategy extends LazyLogging {

}