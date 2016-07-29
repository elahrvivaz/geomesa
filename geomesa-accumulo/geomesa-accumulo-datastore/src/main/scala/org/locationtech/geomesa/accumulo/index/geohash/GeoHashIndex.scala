/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index.geohash

import org.locationtech.geomesa.accumulo.index._
import org.opengis.feature.simple.SimpleFeatureType

@deprecated("z2/z3")
object GeoHashIndex extends AccumuloFeatureIndex {

  override val name: String = "st_idx"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getGeometryDescriptor != null && sft.getSchemaVersion < 8 &&
        (sft.getEnabledTables.isEmpty || sft.getEnabledTables.contains(name))
  }

  override val writable: AccumuloWritableIndex = GeoHashWritableIndex

  override val queryable: AccumuloQueryableIndex = GeoHashQueryableIndex
}
