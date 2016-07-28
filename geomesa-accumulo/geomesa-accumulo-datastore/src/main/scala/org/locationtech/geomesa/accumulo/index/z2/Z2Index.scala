/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index.z2

import org.locationtech.geomesa.accumulo.index._
import org.opengis.feature.simple.SimpleFeatureType

object Z2Index extends AccumuloSplitIndex(Z2Table, Z2IdxStrategy) {

  override val name: String = "z2"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getGeometryDescriptor != null && sft.getSchemaVersion > 7 &&
        (sft.getEnabledTables.isEmpty || sft.getEnabledTables.contains(name))
  }

  override val toString = getClass.getSimpleName.split("\\$").last
}
