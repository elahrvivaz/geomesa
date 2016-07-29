/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.index.api.StrategyDecider
import org.opengis.feature.simple.SimpleFeatureType

object AccumuloStrategyDecider extends
    StrategyDecider[AccumuloDataStore, WritableFeature, Mutation, Text, Entry[Key, Value], QueryPlan] {

  override def indices(sft: SimpleFeatureType): Seq[AccumuloFeatureIndex] = AccumuloFeatureIndex.indices(sft)
}
