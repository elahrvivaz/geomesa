/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import java.util

import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.api.index.{QueryFilter, QueryPlan, _}
import org.locationtech.geomesa.api.utils.{ExplainNull, ExplainQuery}
import org.opengis.feature.simple.SimpleFeatureType

object AccumuloIndexManager extends IndexManager[RowValue, Mutation, Text, java.util.Map[Key, Value]] {

  override def available: Seq[AccumuloIndexStrategy] = ???
}

trait AccumuloIndexStrategy extends IndexStrategy[RowValue, Mutation, Text, java.util.Map[Key, Value]] {
  override def mutable: AccumuloMutableIndex
}

trait AccumuloQueryableIndex extends QueryableIndex[java.util.Map[Key, Value]] {
  override def getQueryPlan(sft: SimpleFeatureType,
                            filter: QueryFilter[java.util.Map[Key, Value]],
                            hints: Hints,
                            explain: ExplainQuery): AccumuloQueryPlan
}

trait AccumuloMutableIndex extends MutableIndex[RowValue, Value, Mutation] {

  type FeatureToMutation = (WritableFeature[RowValue]) => Seq[Mutation]

  def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit
}

class RowValue(val cf: Text, val cq: Text, val vis: ColumnVisibility, toValue: => Value) {
  lazy val value: Value = toValue
}