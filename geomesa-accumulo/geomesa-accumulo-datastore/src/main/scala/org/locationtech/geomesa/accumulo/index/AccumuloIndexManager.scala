/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.locationtech.geomesa.accumulo.index.attribute.AttributeIndex
// noinspection ScalaDeprecation
import org.locationtech.geomesa.accumulo.index.geohash.GeoHashIndex
import org.locationtech.geomesa.accumulo.index.id.RecordIndex
import org.locationtech.geomesa.accumulo.index.z2.Z2Index
import org.locationtech.geomesa.accumulo.index.z3.Z3Index
import org.opengis.feature.simple.SimpleFeatureType

object AccumuloIndexManager {

  // note: keep in priority order for running full table scans
  val AllIndices: Seq[AccumuloFeatureIndex] = {
    // noinspection ScalaDeprecation
    Seq(Z3Index, Z2Index, RecordIndex, AttributeIndex, GeoHashIndex)
  }

  def index(name: String): AccumuloFeatureIndex = AllIndices.find(_.name == name).getOrElse {
    throw new IllegalArgumentException(s"No index with name $name exists")
  }

  def indices(sft: SimpleFeatureType): Seq[AccumuloFeatureIndex] = AllIndices.filter(_.supports(sft))

  object Schemes {
    val Z3TableScheme: List[String] = List(AttributeIndex, RecordIndex, Z3Index).map(_.name)
    val Z2TableScheme: List[String] = List(AttributeIndex, RecordIndex, Z2Index).map(_.name)
  }
}
