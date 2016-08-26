/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.geohash

import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType

@deprecated("z2/z3")
object GeoHashIndex extends AccumuloFeatureIndex with GeoHashWritableIndex with GeoHashQueryableIndex {

  override val name: String = "st_idx"

  override val version: Int = 2

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // note: current version 'supports' is false as this class is deprecated
    // noinspection ExistsEquals - scala 2.10 compatibility
    sft.getGeometryDescriptor != null && sft.getIndexVersion(name).exists(_ == 1)
  }

  // 2  -> sorted keys in the STIDX table
  // 8  -> z2 index, deprecating stidx
  override def getIndexVersion(schemaVersion: Int): Int =
    if (schemaVersion < 2) { 0 } else if (schemaVersion < 8) { 1 } else { 2 }
}
