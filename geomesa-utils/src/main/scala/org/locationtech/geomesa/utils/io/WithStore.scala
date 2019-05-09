/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import org.geotools.data.{DataStore, DataStoreFinder}

class WithStore[DS <: DataStore] private (params: java.util.Map[String, _]) {
  def apply[T](fn: DS => T): T = {
    val ds = DataStoreFinder.getDataStore(params)
    try { fn(ds.asInstanceOf[DS]) } finally {
      if (ds != null) {
        ds.dispose()
      }
    }
  }
}

/**
  * Look up a data store and safely dispose of it when done
  */
object WithStore {

  import scala.collection.JavaConverters._

  def apply[DS <: DataStore](params: Map[String, _]): WithStore[DS] = apply(params.asJava)

  def apply[DS <: DataStore](params: java.util.Map[String, _]): WithStore[DS] = new WithStore[DS](params)
}


