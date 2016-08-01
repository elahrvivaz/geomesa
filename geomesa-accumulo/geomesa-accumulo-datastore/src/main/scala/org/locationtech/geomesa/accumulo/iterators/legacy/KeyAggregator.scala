/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators.legacy

import org.apache.accumulo.core.data.{Key, Value}

trait KeyAggregator {
  def reset(): Unit

  def collect(key: Key, value: Value): Unit

  def aggregate: Value

  def setOpt(opt: String, value: String): Unit
}
