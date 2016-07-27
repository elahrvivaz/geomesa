/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.api.index

import org.locationtech.geomesa.api.stats.GeoMesaStats
import org.locationtech.geomesa.api.utils.{ExplainNull, ExplainQuery}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing, TimingsImpl}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait IndexManager[WriteKeyValue, WriteResult, RowKey, QueryResult] extends MethodProfiling {

  /**
    * Available strategies, in priority order
    *
    * @return
    */
  def available: Seq[IndexStrategy[WriteKeyValue, WriteResult, RowKey, QueryResult]]


  def supported(sft: SimpleFeatureType): Seq[IndexStrategy[WriteKeyValue, WriteResult, RowKey, QueryResult]] =
    available.filter(_.supports(sft))

  /**
    * Creates a function to write a feature to all indices
    */
  def writers(sft: SimpleFeatureType): (WritableFeature[WriteKeyValue]) => Seq[WriteResult] = {
    val writers = supported(sft).map(_.mutable.writer(sft))
    (wf: WritableFeature[WriteKeyValue]) => { writers.flatMap(_.apply(wf)) }
  }

  /**
    * Creates a function to delete a feature from all indices
    */
  def removers(sft: SimpleFeatureType): (WritableFeature[WriteKeyValue]) => Seq[WriteResult] = {
    val removers = supported(sft).map(_.mutable.writer(sft))
    (wf: WritableFeature[WriteKeyValue]) => { removers.flatMap(_.apply(wf)) }
  }

  /**
    * TODO
    *
    * @param sft
    * @param stats
    * @param filter
    * @param transform
    * @param explain
    * @return
    */
  def getQueryFilters(sft: SimpleFeatureType,
                      stats: Option[GeoMesaStats],
                      filter: Filter,
                      transform: Option[SimpleFeatureType],
                      explain: ExplainQuery = ExplainNull): Seq[QueryFilter[QueryResult]] = {

    implicit val timings = new TimingsImpl()

    val supported = available.filter(_.supports(sft)).map(_.queryable)
    val options = profile(new QueryFilterSplitter(sft, supported).getQueryOptions(filter), "split")

    explain(s"Query processing took ${timings.time("split")}ms and produced ${options.length} options")

    val selected = profile({
      if (options.isEmpty) {
        explain("No filter plans found")
        Seq.empty // corresponds to filter.exclude
      } else {
        val filterPlan = if (options.length == 1) {
          // only a single option, so don't bother with cost
          explain(s"Filter plan: ${options.head}")
          options.head
        } else {
          // choose the best option based on cost
          val costs = options.map { option =>
            val timing = new Timing
            val optionCosts = profile(option.filters.map(f => f.index.getCost(sft, stats, _, transform)))(timing)
            (option, optionCosts.sum, timing.time)
          }.sortBy(_._2)
          val (cheapest, cost, time) = costs.head
          explain(s"Filter plan selected: $cheapest (Cost: $cost in ${time}ms with${if (stats.isEmpty) "out"} stats)")
          explain(s"Filter plans not used (${costs.size - 1}):",
            costs.drop(1).map(c => s"${c._1} (Cost ${c._2} in ${c._3}ms with${if (stats.isEmpty) "out"} stats)"))
          cheapest
        }
        filterPlan.filters
      }
    }, "cost")

    explain(s"Strategy selection took ${timings.time("cost")}ms for ${options.length} options")

    selected
  }
}
