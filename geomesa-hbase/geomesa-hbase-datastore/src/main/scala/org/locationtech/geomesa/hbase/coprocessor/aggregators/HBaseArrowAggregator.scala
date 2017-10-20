/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.ArrowEncodedSft
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, QueryPlan}
import org.locationtech.geomesa.index.iterators.ArrowScan
import org.locationtech.geomesa.index.iterators.ArrowScan.ArrowAggregate
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class HBaseArrowAggregator extends ArrowScan with HBaseAggregator[ArrowAggregate]

object HBaseArrowAggregator {

  def bytesToFeatures(bytes: Array[Byte]): SimpleFeature =
    new ScalaSimpleFeature(ArrowEncodedSft, "", Array(bytes, GeometryUtils.zeroPoint))

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                stats: GeoMesaStats,
                filter: Option[Filter],
                hints: Hints): (Map[String, String], QueryPlan.Reducer) = {
    val conf = ArrowScan.configure(sft, index, stats, filter, hints)
    (conf.config ++ Map(GeoMesaCoprocessor.AggregatorClass -> classOf[HBaseArrowAggregator].getName), conf.reduce)
  }
}
