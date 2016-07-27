/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.api.index

import org.locationtech.geomesa.filter._
import org.opengis.filter.Filter


/**
  * Filters split into a 'primary' that will be used for range planning,
  * and a 'secondary' that will be applied as a final step.
  *
  * @param index strategy used to satisfy this filter
  * @param dimensions number of dimensions being queried - higher is implied to be more precise
  * @param primary the part of the filter used for query planning
  * @param secondary the part of the filter not used for query planning
  */
case class QueryFilter[QueryResult](index: QueryableIndex[QueryResult],
                                    dimensions: Int,
                                    primary: Option[Filter],
                                    secondary: Option[Filter] = None) {

  lazy val filter: Option[Filter] = andOption(primary.toSeq ++ secondary)

  override lazy val toString: String =
    s"$index[${primary.map(filterToString).getOrElse("INCLUDE")}]" +
        s"[${secondary.map(filterToString).getOrElse("None")}]"
}

/**
  * A series of queries required to satisfy a filter - basically split on ORs
  */
case class FilterPlan[QueryResult](filters: Seq[QueryFilter[QueryResult]]) {
  override lazy val toString: String = s"FilterPlan[${filters.mkString(",")}]"
}

object FilterPlan {
  def apply[QueryResult](filter: QueryFilter[QueryResult]): FilterPlan[QueryResult] = FilterPlan(Seq(filter))
}
