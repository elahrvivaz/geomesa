/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import org.geotools.data.{DataStore, Transaction}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.HasGeoMesaStats

package object geotools {

  type GeoMeasBaseStore = DataStore with HasGeoMesaFeatureReader with HasGeoMesaStats

  /**
   * Transaction object that enforces atomic writes - this ensures that a feature is not modified between
   * when it's read and when it's updated. Does not support normal transaction operations, such
   * as commit or rollback, and instead operates like auto-commit.
   */
  val AtomicWrites: Transaction = AtomicTransaction

  private object AtomicTransaction extends Transaction {

    override def putState(key: Any, state: Transaction.State): Unit =
      throw new UnsupportedOperationException("PutState is not supported for GeoMesa stores")

    override def removeState(key: Any): Unit =
      throw new UnsupportedOperationException("RemoveState is not supported for GeoMesa stores")

    override def getState(key: Any): Transaction.State =
      throw new UnsupportedOperationException("GetState is not supported for GeoMesa stores")

    override def addAuthorization(authID: String): Unit =
      throw new UnsupportedOperationException("AddAuthorization is not supported for GeoMesa stores")

    override def getAuthorizations: java.util.Set[String] =
      throw new UnsupportedOperationException("GetAuthorizations is not supported for GeoMesa stores")

    override def putProperty(key: Any, value: Any): Unit =
      throw new UnsupportedOperationException("PutProperty is not supported for GeoMesa stores")

    override def getProperty(key: Any): AnyRef =
      throw new UnsupportedOperationException("GetProperty is not supported for GeoMesa stores")

    override def commit(): Unit = {}

    override def rollback(): Unit =
      throw new UnsupportedOperationException("Rollback is not supported for GeoMesa stores")

    override def close(): Unit = {}

    override def toString: String = "AtomicTransaction"
  }
}
