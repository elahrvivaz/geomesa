/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.api

import org.geotools.data.Query
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing, TimingsImpl}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait QueryStrategyDecider {
  def chooseFilterPlan(sft: SimpleFeatureType,
                       query: Query,
                       stats: GeoMesaStats,
                       requested: Option[GeoMesaFeatureIndex],
                       output: ExplainerOutputType = ExplainNull): FilterPlan
}

object QueryStrategyDecider extends QueryStrategyDecider with MethodProfiling {

  /**
   * Selects a strategy for executing a given query.
   *
   * If a particular strategy has been requested, that strategy will be used (note - this is only
   * partially supported, and should be used with care.)
   *
   * Otherwise, the query will be examined for strategies that could be used to execute it. The cost of
   * executing each available strategy will be calculated, and the least expensive strategy will be used.
   *
   * Currently, the costs are hard-coded to conform to the following priority:
   *
   *  * If an ID predicate is present, then use the record index strategy
   *  * If high cardinality attribute filters are present, then use the attribute index strategy
   *  * If a date filter is present, then use the Z3 index strategy
   *  * If a spatial filter is present, then use the ST index strategy
   *  * If other attribute filters are present, then use the attribute index strategy
   *  * If none of the above, use the record index strategy (likely a full table scan)
   *
   */
  override def chooseFilterPlan(sft: SimpleFeatureType,
                                query: Query,
                                stats: GeoMesaStats,
                                requested: Option[AccumuloFeatureIndex],
                                output: ExplainerOutputType): FilterPlan = {

    implicit val timings = new TimingsImpl()

    // get the various options that we could potentially use
    val options = {
      val all = profile(new QueryFilterSplitter(sft).getQueryOptions(query.getFilter), "split")
      // don't evaluate z2 index if there is a z3 option, it will always be picked
      // TODO if we eventually take into account date range, this will need to be removed
      if (requested.isEmpty && all.exists(_.strategies.forall(_.index == Z3Index)) &&
          all.exists(_.strategies.forall(_.index == Z2Index))) {
        all.filterNot(_.strategies.forall(_.index == Z2Index))
      } else {
        all
      }
    }

    output(s"Query processing took ${timings.time("split")}ms and produced ${options.length} options")

    val selected = profile({
      if (requested.isDefined) {
        val forced = forceStrategy(options, requested.get, query.getFilter)
        output(s"Filter plan forced to $forced")
        forced
      } else if (options.isEmpty) {
        output("No filter plans found")
        FilterPlan(Seq.empty) // corresponds to filter.exclude
      } else if (options.length == 1) {
        // only a single option, so don't bother with cost
        output(s"Filter plan: ${options.head}")
        options.head
      } else {
        // choose the best option based on cost
        val evaluation = query.getHints.getCostEvaluation match {
          case CostEvaluation.Stats => Some(stats)
          case CostEvaluation.Index => None
        }
        val transform = query.getHints.getTransformSchema
        val costs = options.map { option =>
          val timing = new Timing
          val optionCosts = profile(option.strategies.map(f => f.index.getCost(sft, evaluation, f, transform)))(timing)
          (option, optionCosts.sum, timing.time)
        }.sortBy(_._2)
        val (cheapest, cost, time) = costs.head
        output(s"Filter plan selected: $cheapest (Cost $cost)(Cost evaluation: $evaluation in ${time}ms)")
        output(s"Filter plans not used (${costs.size - 1}):",
          costs.drop(1).map(c => s"${c._1} (Cost ${c._2} in ${c._3}ms)"))
        cheapest
      }
    }, "cost")

    output(s"Strategy selection took ${timings.time("cost")}ms for ${options.length} options")

    selected
  }

  // see if one of the normal plans matches the requested type - if not, force it
  private def forceStrategy(options: Seq[FilterPlan], index: AccumuloFeatureIndex, allFilter: Filter): FilterPlan = {
    def checkStrategy(f: FilterStrategy) = f.index == index
    options.find(_.strategies.forall(checkStrategy)).getOrElse {
      val secondary = if (allFilter == Filter.INCLUDE) None else Some(allFilter)
      FilterPlan(Seq(FilterStrategy(index, None, secondary)))
    }
  }
}
