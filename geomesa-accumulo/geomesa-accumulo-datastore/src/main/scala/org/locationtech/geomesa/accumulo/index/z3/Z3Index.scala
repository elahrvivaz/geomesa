/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import org.locationtech.geomesa.accumulo.index._
import org.opengis.feature.simple.SimpleFeatureType

object Z3Index extends AccumuloFeatureIndex {

  override val name: String = "z3"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && ((sft.getSchemaVersion > 6 && sft.getGeometryDescriptor != null) ||
        (sft.getSchemaVersion > 4 && sft.isPoints)) && sft.isTableEnabled(name)
  }

  override val writable: AccumuloWritableIndex = Z3WritableIndex

  override val queryable: AccumuloQueryableIndex = Z3QueryableIndex
}
