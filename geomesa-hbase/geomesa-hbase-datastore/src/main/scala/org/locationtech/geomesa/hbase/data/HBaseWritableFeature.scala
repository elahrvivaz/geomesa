/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.hbase.data

import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.hashing.MurmurHash3

class RowValue(val cf: Array[Byte], val cq: Array[Byte], toValue: => Array[Byte]) {
  lazy val value: Array[Byte] = toValue
}

class HBaseWritableFeature(val feature: SimpleFeature,
                           sft: SimpleFeatureType,
                           serializer: SimpleFeatureSerializer,
                           indexSerializer: SimpleFeatureSerializer) {

  import HBaseFeatureIndex.{DataColumnFamily, EmptyBytes}

  lazy val fullValue = new RowValue(DataColumnFamily, EmptyBytes, serializer.serialize(feature))

  lazy val indexValue = new RowValue(DataColumnFamily, EmptyBytes, indexSerializer.serialize(feature))

  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))
}