/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import org.apache.kudu.client.{KuduPredicate, PartialRow}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.kudu.schema.KuduResultAdapter
import org.locationtech.geomesa.kudu.schema.KuduResultAdapter.EmptyAdapter
import org.locationtech.geomesa.kudu.utils.KuduBatchScan
import org.locationtech.geomesa.kudu.{KuduFilterStrategyType, KuduQueryPlanType}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

sealed trait KuduQueryPlan extends KuduQueryPlanType {
  def filter: KuduFilterStrategyType
  def table: String
  def ranges: Seq[(Option[PartialRow], Option[PartialRow])]
  def predicates: Seq[KuduPredicate]
  def ecql: Option[Filter]
  def adapter: KuduResultAdapter
  def numThreads: Int

  override def explain(explainer: Explainer, prefix: String): Unit =
    KuduQueryPlan.explain(this, explainer, prefix)
}

object KuduQueryPlan {

  def explain(plan: KuduQueryPlan, explainer: Explainer, prefix: String): Unit = {
    import org.locationtech.geomesa.filter.filterToString
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${Option(plan.table).orNull}")
    explainer(s"Columns: ${plan.adapter.columns.mkString(", ")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(_.toString).mkString(", ")}")
    explainer(s"Additional predicates: ${if (plan.predicates.isEmpty) { "None" } else { plan.predicates.map(_.toString).mkString(", ") }}")
    explainer(s"Client-side filter: ${plan.ecql.map(filterToString).getOrElse("None")}")
    explainer(s"Rows to features: ${plan.adapter}")
    explainer.popLevel()
  }

  // plan that will not actually scan anything
  case class EmptyPlan(filter: KuduFilterStrategyType) extends KuduQueryPlan {
    override def table: String = ""
    override def ranges: Seq[(Option[PartialRow], Option[PartialRow])] = Seq.empty
    override def predicates: Seq[KuduPredicate] = Seq.empty
    override def ecql: Option[Filter] = None
    override def numThreads: Int = 0
    override def adapter: KuduResultAdapter = EmptyAdapter
    override def scan(ds: KuduDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
  }

  case class ScanPlan(filter: KuduFilterStrategyType,
                      table: String,
                      ranges: Seq[(Option[PartialRow], Option[PartialRow])],
                      predicates: Seq[KuduPredicate],
                      // note: filter is applied in entriesToFeatures, this is just for explain logging
                      ecql: Option[Filter],
                      adapter: KuduResultAdapter,
                      numThreads: Int) extends KuduQueryPlan {

    override val hasDuplicates: Boolean = false

    override def scan(ds: KuduDataStore): CloseableIterator[SimpleFeature] = {
      import scala.collection.JavaConverters._
      val scan = new KuduBatchScan(ds.client, table, adapter.columns, ranges, predicates, numThreads, 1000)
      adapter.adapt(scan.flatMap(_.iterator.asScala))
    }
  }
}
