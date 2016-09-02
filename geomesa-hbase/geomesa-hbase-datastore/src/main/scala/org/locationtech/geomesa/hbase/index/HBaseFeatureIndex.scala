/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client.Put
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseWritableFeature}
import org.locationtech.geomesa.index.api.{FilterPlan, FilterStrategy, GeoMesaFeatureIndex}

object HBaseFeatureIndex {

  type HBaseFeatureIndex = GeoMesaFeatureIndex[HBaseDataStore, HBaseWritableFeature, Put, QueryPlan]
  type HBaseFilterPlan = FilterPlan[HBaseDataStore, HBaseWritableFeature, Put, QueryPlan]
  type HBaseFilterStrategy = FilterStrategy[HBaseDataStore, HBaseWritableFeature, Put, QueryPlan]
}
