/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index.z3

import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object Z3Index extends DelegatingFeatureIndex(Z3Table, Z3IdxStrategy) {

  override val name: String = "z3"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && ((sft.getSchemaVersion > 6 && sft.getGeometryDescriptor != null) ||
        (sft.getSchemaVersion > 4 && sft.isPoints)) && (sft.getEnabledTables.isEmpty || sft.getEnabledTables.contains(name))
  }
}
