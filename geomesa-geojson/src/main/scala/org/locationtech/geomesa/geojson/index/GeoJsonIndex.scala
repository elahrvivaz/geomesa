/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.geojson.index

import org.codehaus.jackson.JsonNode
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.api.{AccumuloGeoMesaIndex, DefaultSimpleFeatureView}

class GeoJsonIndex(name: String, ds: AccumuloDataStore) {

  private val index = new AccumuloGeoMesaIndex(ds, name, new GeoJsonSerializer, new DefaultSimpleFeatureView[JsonNode])


}
