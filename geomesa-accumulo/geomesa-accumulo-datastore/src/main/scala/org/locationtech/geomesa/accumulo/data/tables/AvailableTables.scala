/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import org.locationtech.geomesa.accumulo.index.attribute.AttributeIndex
import org.locationtech.geomesa.accumulo.index.id.RecordIndex
import org.locationtech.geomesa.accumulo.index.z2.Z2Index
import org.locationtech.geomesa.accumulo.index.z3.Z3Index

object AvailableTables {
  // TODO move this
  val Z3TableScheme: List[String] = List(AttributeIndex, RecordIndex, Z3Index).map(_.name)
  val Z2TableScheme: List[String] = List(AttributeIndex, RecordIndex, Z2Index).map(_.name)
}