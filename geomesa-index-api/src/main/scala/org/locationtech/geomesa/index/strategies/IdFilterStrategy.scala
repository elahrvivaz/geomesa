/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.index.strategies

import org.locationtech.geomesa.filter.visitor.IdExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaIndexQueryable}
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait IdFilterStrategy[Ops <: HasGeoMesaStats, FeatureWrapper, Result, Row, Entries, Plan] extends
    GeoMesaIndexQueryable[Ops, FeatureWrapper, Result, Row, Entries, Plan] {

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter):
      Seq[FilterStrategy[Ops, FeatureWrapper, Result, Row, Entries, Plan]] = {
    if (filter == Filter.INCLUDE) {
      Seq(FilterStrategy(index, None, None))
    } else if (filter == Filter.EXCLUDE) {
      Seq.empty
    } else {
      val (ids, notIds) = IdExtractingVisitor(filter)
      if (ids.isDefined) {
        Seq(FilterStrategy(index, ids, notIds))
      } else {
        Seq(FilterStrategy(index, None, Some(filter)))
      }
    }
  }

  // top-priority index - always 1 if there are actually ID filters
  override def getCost(sft: SimpleFeatureType,
                       ops: Option[Ops],
                       filter: FilterStrategy[Ops, FeatureWrapper, Result, Row, Entries, Plan],
                       transform: Option[SimpleFeatureType]): Long = {
    if (filter.primary.isDefined) IdFilterStrategy.StaticCost else Long.MaxValue
  }
}

object IdFilterStrategy {
  val StaticCost = 1L
}