/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.data.{Mutation, Range}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.accumulo.iterators.Z2Iterator
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexValues}
import org.opengis.feature.simple.SimpleFeatureType

// current version - new z-curve
case object Z2Index extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with Z2Index[AccumuloDataStore, AccumuloFeature, Mutation, Range, ScanConfig] {

  val Z2IterPriority = 23

  override val version: Int = 4

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override protected def updateScanConfig(sft: SimpleFeatureType,
                                          config: ScanConfig,
                                          indexValues: Option[Z2IndexValues]): ScanConfig = {
    indexValues match {
      case None => config
      case Some(Z2IndexValues(sfc, _, xy)) =>
        val zIter = Z2Iterator.configure(sft, sfc, xy, Z2IterPriority)
        config.copy(iterators = config.iterators :+ zIter)
    }
  }
}