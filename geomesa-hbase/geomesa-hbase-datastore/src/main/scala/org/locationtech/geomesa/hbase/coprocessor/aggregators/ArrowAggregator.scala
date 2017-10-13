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
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.ArrowScan
import org.locationtech.geomesa.index.iterators.ArrowScan.ArrowAggregate
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ArrowAggregator extends ArrowScan with HBaseAggregator[ArrowAggregate]

object ArrowAggregator {

  def bytesToFeatures(bytes: Array[Byte]): SimpleFeature =
    new ScalaSimpleFeature(ArrowEncodedSft, "", Array(bytes, GeometryUtils.zeroPoint))

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                dictionaries: Seq[String],
                hints: Hints): Map[String, String] = {
    ArrowScan.configure(sft, index, filter, dictionaries, hints) ++
        Map(GeoMesaCoprocessor.AggregatorClass -> classOf[ArrowAggregator].getName)
  }
}
