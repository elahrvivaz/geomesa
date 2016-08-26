/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType

object Z3Index extends AccumuloFeatureIndex with Z3WritableIndex with Z3QueryableIndex {

  val Z3IterPriority = 23

  // the bytes of z we keep for complex geoms
  // 3 bytes is 15 bits of geometry (not including time bits and the first 2 bits which aren't used)
  // roughly equivalent to 3 digits of geohash (32^3 == 2^15) and ~78km resolution
  // (4 bytes is 20 bits, equivalent to 4 digits of geohash and ~20km resolution)
  // note: we also lose time resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long = Long.MaxValue << (64 - 8 * GEOM_Z_NUM_BYTES)
  // step needed (due to the mask) to bump up the z value for a complex geom
  val GEOM_Z_STEP: Long = 1L << (64 - 8 * GEOM_Z_NUM_BYTES)

  // geoms always have splits, but they weren't added until schema 7
  def hasSplits(sft: SimpleFeatureType) = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getIndexVersion(name).exists(_ > 1)
  }

  override val name: String = "z3"

  // 1 -> initial implementation
  // 2 -> polygon support and splits
  // 3 -> deprecated polygon support
  override val version: Int = 3

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && sft.getIndexVersion(name).exists {
      case 3 if sft.isPoints => true
      case 2 if sft.getGeometryDescriptor != null => true
      case 1 if sft.isPoints => true
      case _ => false
    }
  }

  // 5  -> z3 index
  // 7  -> z3 polygons
  // 10 -> XZ3 index
  override def getIndexVersion(schemaVersion: Int): Int =
    if (schemaVersion < 5) { 0 } else if (schemaVersion < 7) { 1 } else if (schemaVersion < 10) { 2 } else { 3 }
}
