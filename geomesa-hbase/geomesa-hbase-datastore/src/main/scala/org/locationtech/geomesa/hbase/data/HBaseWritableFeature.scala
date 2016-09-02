/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.hbase.data

import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.accumulo.index.encoders.BinEncoder
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils._
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.hashing.MurmurHash3

class RowValue(val cf: Text, val cq: Text, val vis: ColumnVisibility, toValue: => Value) {
  lazy val value: Value = toValue
}

class HBaseWritableFeature(val feature: SimpleFeature,
                           sft: SimpleFeatureType,
                           serializer: SimpleFeatureSerializer,
                           indexSerializer: SimpleFeatureSerializer) {

  import AccumuloWritableIndex.{BinColumnFamily, EmptyColumnQualifier, FullColumnFamily, IndexColumnFamily}
  import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

  private lazy val visibility =
    new ColumnVisibility(feature.userData[String](FEATURE_VISIBILITY).getOrElse(defaultVisibility))

  lazy val fullValues: Seq[RowValue] =
    Seq(new RowValue(FullColumnFamily, EmptyColumnQualifier, visibility, new Value(serializer.serialize(feature))))

  lazy val indexValues: Seq[RowValue] =
    Seq(new RowValue(IndexColumnFamily, EmptyColumnQualifier, visibility, new Value(indexSerializer.serialize(feature))))

  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))
}