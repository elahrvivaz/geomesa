/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value, Range => aRange}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.AccumuloQueryPlan.{JoinFunction, ResultEntry}
import org.locationtech.geomesa.api.index.{QueryFilter, QueryPlan}
import org.opengis.feature.simple.SimpleFeature

object AccumuloQueryPlan {
  type ResultEntry = java.util.Map.Entry[Key, Value]
  type JoinFunction = (java.util.Map.Entry[Key, Value]) => aRange
}

sealed trait AccumuloQueryPlan extends QueryPlan[ResultEntry]{
  def table: String
  def ranges: Seq[aRange]
  def iterators: Seq[IteratorSetting]
  def columnFamilies: Seq[Text]
  def numThreads: Int

  def join: Option[(JoinFunction, AccumuloQueryPlan)] = None
}

// plan that will not actually scan anything
case class EmptyPlan(filter: QueryFilter[ResultEntry]) extends AccumuloQueryPlan {
  override val table: String = ""
  override val iterators: Seq[IteratorSetting] = Seq.empty
  override val ranges: Seq[aRange] = Seq.empty
  override val columnFamilies: Seq[Text] = Seq.empty
  override val hasDuplicates: Boolean = false
  override val numThreads: Int = 0

  override def resultsToFeatures: (ResultEntry) => SimpleFeature = (_) => null
}

// single scan plan
case class ScanPlan(filter: QueryFilter[ResultEntry],
                    table: String,
                    range: aRange,
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    resultsToFeatures: (ResultEntry) => SimpleFeature,
                    hasDuplicates: Boolean) extends AccumuloQueryPlan {
  override val numThreads = 1
  override val ranges = Seq(range)
}

// batch scan plan
case class BatchScanPlan(filter: QueryFilter[ResultEntry],
                         table: String,
                         ranges: Seq[aRange],
                         iterators: Seq[IteratorSetting],
                         columnFamilies: Seq[Text],
                         resultsToFeatures: (ResultEntry) => SimpleFeature,
                         numThreads: Int,
                         hasDuplicates: Boolean) extends AccumuloQueryPlan

// join on multiple tables - requires multiple scans
case class JoinPlan(filter: QueryFilter[ResultEntry],
                    table: String,
                    ranges: Seq[aRange],
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    numThreads: Int,
                    hasDuplicates: Boolean,
                    joinFunction: JoinFunction,
                    joinQuery: BatchScanPlan) extends AccumuloQueryPlan {
  override def resultsToFeatures: (ResultEntry) => SimpleFeature = joinQuery.resultsToFeatures
  override val join = Some((joinFunction, joinQuery))
}