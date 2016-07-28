/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.index.attribute.{AttributeIndex, AttributeIdxStrategy}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.IdDetectingFilterVisitor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Class for splitting queries up based on Boolean clauses and the available query strategies.
 */
class QueryFilterSplitter(sft: SimpleFeatureType) extends LazyLogging {

  import QueryFilterSplitter._

  /**
    * Splits the query up into different filter plans to be evaluated. Each filter plan will consist of one or
    * more query plans. Each query plan will have a primary part (that would be used for query planning)
    * and an optional secondary part (that would be applied as a secondary filter).
    *
    * Examples:
    *
    * bbox(geom) AND attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(ST,Some([ geom bbox ]),Some([ attr1 = ? ])))))
    *
    * bbox(geom) OR attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(ST,Some([ geom bbox ]),None), QueryFilter(ATTRIBUTE,Some([ attr1 = ? ]),None))))
    *
    * bbox(geom) AND dtg DURING ? AND attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(Z3,Some([ geom bbox AND dtg during ? ]),Some([ attr1 = ? ])))),
    *     FilterPlan(Seq(QueryFilter(ATTRIBUTE,Some([ attr1 = ? ]),Some([ geom bbox AND dtg during ? ])))))
    *
    * note: spatial and temporal filters are combined.
    *
    * (bbox(geom) OR geom INTERSECT) AND attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(ST,Some([ geom bbox OR geom intersect ]),Some([ attr1 = ? ]))))
    *     FilterPlan(Seq(QueryFilter(ATTRIBUTE,Seq([ attr1 = ? ]),Some([ geom bbox OR geom intersect ])))))
    *
    * note: ORs will not be split if they operate on a single attribute
    *
    */
  def getQueryOptions(filter: Filter): Seq[FilterPlan] = {
    // cnf gives us a top level AND with ORs as first children
    rewriteFilterInCNF(filter) match {
      case a: And =>
        // look for ORs across attributes, e.g. bbox OR dtg
        val (complex, simple) = a.getChildren.partition(f => f.isInstanceOf[Or] && attributeAndIdCount(f) > 1)
        if (complex.isEmpty) {
          // no cross-attribute ORs
          getSimpleQueryOptions(a).map(FilterPlan.apply)
        } else if (simple.nonEmpty) {
          logger.warn("Not considering complex OR predicates in query planning: " +
              s"${complex.map(filterToString).mkString("(", ") AND (", ")")}")
          def addComplexPredicates(qf: FilterStrategy) = qf.copy(secondary = andOption(qf.secondary.toSeq ++ complex))
          getSimpleQueryOptions(andFilters(simple)).map(addComplexPredicates).map(FilterPlan.apply)
        } else {
          logger.warn(s"Falling back to expand/reduce query splitting for filter ${filterToString(filter)}")
          val dnf = rewriteFilterInDNF(filter).asInstanceOf[Or]
          expandReduceOrOptions(dnf).map(makeDisjoint)
        }

      case o: Or =>
        // there are no ands - just ors between fields
        // this implies that each child has only a single property or ID
        def getGroup(f: Filter) = (FilterHelper.propertyNames(f, sft), FilterHelper.hasIdFilter(f))

        // group and then recombine the OR'd filters by the attribute they operate on
        val groups = o.getChildren.groupBy(getGroup).values.map(ff.or(_)).toSeq
        val perAttributeOptions = groups.flatMap { g =>
          val options = getSimpleQueryOptions(g)
          require(options.length < 2, s"Expected only a single option for ${filterToString(g)} but got $options")
          options.headOption
        }
        if (perAttributeOptions.exists(_.primary.isEmpty)) {
          // we have to do a full table scan for part of the query, just append everything to that
          Seq(FilterPlan(fullTableScanOption(o)))
        } else {
          Seq(makeDisjoint(FilterPlan(perAttributeOptions)))
        }

      case f =>
        getSimpleQueryOptions(f).map(qf => FilterPlan(Seq(qf)))
    }
  }

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    *   (bbox1 OR bbox2) AND dtg
    *   bbox AND dtg AND (attr1 = foo OR attr = bar)
    * not:
    *   bbox OR dtg
    *
    * Because the inputs are simple, each one can be satisfied with a single query filter.
    * The returned values will each satisfy the query, using a different strategy.
    *
    * @param filter input filter
    * @return sequence of options, any of which can satisfy the query
    */
  private def getSimpleQueryOptions(filter: Filter): Seq[FilterStrategy] = {
    val options = IndexManager.indices(sft).flatMap(_.getFilterStrategy(sft, filter))
    if (options.isEmpty) {
      Seq.empty
    } else {
      val (fullScans, indexScans) = options.partition(_.primary.isEmpty)
      if (indexScans.nonEmpty) {
        indexScans
      } else {
        Seq(fullScans.head)
      }
    }
  }

  /**
    * Calculates all possible options for each part of the filter, then determines all permutations of
    * the options. This can end up being expensive (O(2&#94;n)), so is only used as a fall-back.
    */
  private def expandReduceOrOptions(filter: Or): Seq[FilterPlan] = {

    // for each child of the or, get the query options
    // each filter plan should only have a single query filter
    def getChildOptions: Seq[Seq[FilterPlan]] =
      filter.getChildren.map(getSimpleQueryOptions(_).map(qf => FilterPlan(Seq(qf))))

    // combine the filter plans so that each plan has multiple query filters
    // use the permutations of the different options for each child
    // TODO GEOMESA-941 Fix algorithmically dangerous (2^N exponential runtime)
    def reduceChildOptions(childOptions: Seq[Seq[FilterPlan]]): Seq[FilterPlan] =
      childOptions.reduce { (left, right) =>
        left.flatMap(l => right.map(r => FilterPlan(l.strategies ++ r.strategies)))
      }

    // try to combine query filters in each filter plan if they have the same primary filter
    // this avoids scanning the same ranges twice with different secondary predicates
    def combineSecondaryFilters(options: Seq[FilterPlan]): Seq[FilterPlan] = options.map { r =>
      // build up the result array instead of using a group by to preserve filter order
      val groups = ArrayBuffer.empty[FilterStrategy]
      r.strategies.distinct.foreach { f =>
        val i = groups.indexWhere(g => g.index == f.index && g.primary == f.primary)
        if (i == -1) {
          groups.append(f)
        } else {
          val current = groups(i).secondary match {
            case Some(o) if o.isInstanceOf[Or] => o.asInstanceOf[Or].getChildren.toSeq
            case Some(n) => Seq(n)
            case None => Seq.empty
          }
          groups.update(i, f.copy(secondary = orOption(current ++ f.secondary)))
        }
      }
      FilterPlan(groups)
    }

    // if a filter plan has any query filters that scan a subset of the range of a different query filter,
    // then we can combine them, as we have to scan the larger range anyway
    def mergeOverlappedFilters(options: Seq[FilterPlan]): Seq[FilterPlan] = options.map { filterPlan =>
      val filters = ArrayBuffer(filterPlan.strategies: _*)
      var merged: FilterStrategy = null
      var i = 0
      while (i < filters.length) {
        val toMerge = filters(i)
        if (toMerge != null) {
          var j = 0
          while (j < filters.length && merged == null) {
            if (i != j) {
              val mergeTo = filters(j)
              if (mergeTo != null) {
                merged = tryMerge(toMerge, mergeTo)
              }
            }
            j += 1
          }
          if (merged != null) {
            // remove the merged query filter and replace the one merged into
            filters.update(i, null)
            filters.update(j - 1, merged)
            merged = null
          }
        }
        i += 1
      }

      // if we have replaced anything, recreate the filter plan
      val overlapped = filters.filter(_ != null)
      if (overlapped.length < filterPlan.strategies.length) {
        FilterPlan(overlapped)
      } else {
        filterPlan
      }
    }

    val childOpts   = getChildOptions
    val reducedOpts = reduceChildOptions(childOpts)
    val combinedSec = combineSecondaryFilters(reducedOpts)
    val merged      = mergeOverlappedFilters(combinedSec)
    if (merged.nonEmpty) {
      merged
    } else {
      Seq(FilterPlan(Seq(fullTableScanOption(filter))))
    }
  }

  /**
    * Will perform a full table scan - used when we don't have anything better. Currently z3, z2 and record
    * tables support full table scans.
    */
  private def fullTableScanOption(filter: Filter): FilterStrategy = {
    val secondary = if (filter == Filter.INCLUDE) None else Some(filter)
    val options = IndexManager.indices(sft).toStream.flatMap(_.getFilterStrategy(sft, Filter.INCLUDE))
    options.headOption.map(o => o.copy(secondary = secondary)).getOrElse {
      throw new UnsupportedOperationException(s"Configured indices do not support the query ${filterToString(filter)}")
    }
  }

  /**
    * Gets the count of distinct attributes being queried - ID is treated as an attribute
    */
  private def attributeAndIdCount(filter: Filter): Int = {
    val attributeCount = FilterHelper.propertyNames(filter, sft).size
    val idCount = if (filter.accept(new IdDetectingFilterVisitor, false).asInstanceOf[Boolean]) 1 else 0
    attributeCount + idCount
  }
}

object QueryFilterSplitter {

  /**
   * Try to merge the two query filters. Return the merged query filter if successful, else null.
   */
  private def tryMerge(toMerge: FilterStrategy, mergeTo: FilterStrategy): FilterStrategy = {
    if (mergeTo.primary.forall(_ == Filter.INCLUDE)) {
      // this is a full table scan, we can just append the OR to the secondary filter
      val secondary = orOption(mergeTo.secondary.toSeq ++ toMerge.filter)
      mergeTo.copy(secondary = secondary)
    } else if (toMerge.index == AttributeIndex && mergeTo.index == AttributeIndex) {
      AttributeIdxStrategy.tryMergeAttrStrategy(toMerge, mergeTo)
    } else {
      // overlapping geoms, date ranges, attribute ranges, etc will be handled when extracting bounds
      null
    }
  }

  /**
    * Make a filter plan disjoint ORs - this way we don't have to deduplicate results
    *
    * @param option filter plan
    * @return same filter plan with disjoint ORs
    */
  private def makeDisjoint(option: FilterPlan): FilterPlan = {
    if (option.strategies.length < 2) {
      option
    } else {
      // A OR B OR C becomes... A OR (B NOT A) OR (C NOT A and NOT B)
      def extractNot(qp: FilterStrategy) = qp.filter.map(ff.not)
      // keep track of our current disjoint clause
      val nots = ArrayBuffer[Filter]()
      extractNot(option.strategies.head).foreach(nots.append(_))
      val filters = Seq(option.strategies.head) ++ option.strategies.tail.map { filter =>
        val sec = Some(andFilters(nots ++ filter.secondary))
        extractNot(filter).foreach(nots.append(_)) // note - side effect
        filter.copy(secondary = sec)
      }
      FilterPlan(filters)
    }
  }
}

/**
 * Filters split into a 'primary' that will be used for range planning,
 * and a 'secondary' that will be applied as a final step.
 */
case class FilterStrategy(index: AccumuloFeatureIndex, primary: Option[Filter], secondary: Option[Filter] = None) {

  lazy val filter: Option[Filter] = andOption(primary.toSeq ++ secondary)

  override lazy val toString: String =
    s"$index[${primary.map(filterToString).getOrElse("INCLUDE")}]" +
        s"[${secondary.map(filterToString).getOrElse("None")}]"
}

/**
 * A series of queries required to satisfy a filter - basically split on ORs
 */
case class FilterPlan(strategies: Seq[FilterStrategy]) {
  override lazy val toString: String = s"FilterPlan[${strategies.mkString(",")}]"
}

object FilterPlan {
  def apply(filter: FilterStrategy): FilterPlan = FilterPlan(Seq(filter))
}
