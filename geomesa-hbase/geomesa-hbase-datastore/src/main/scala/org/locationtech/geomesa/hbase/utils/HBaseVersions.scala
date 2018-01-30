/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}

/**
  * Reflection wrapper for AdminUtils methods between kafka versions 0.9 and 0.10
  */
object HBaseVersions {

  private val hTableDescriptorMethods = classOf[HTableDescriptor].getDeclaredMethods

  def addFamily(descriptor: HTableDescriptor, family: HColumnDescriptor): Unit = _addFamily(descriptor, family)

  private val _addFamily: (HTableDescriptor, HColumnDescriptor) => Unit = {
    val method = hTableDescriptorMethods.find(_.getName == "addFamily").getOrElse {
      throw new NoSuchMethodException("Couldn't find HTableDescriptor.addFamily method")
    }
    val parameterTypes = method.getParameterTypes
    if (parameterTypes.length == 1 && parameterTypes.head == classOf[HColumnDescriptor]) {
      (descriptor, family) => method.invoke(descriptor, family)
    } else {
      throw new NoSuchMethodException(s"Couldn't find HTableDescriptor.addFamily method with correct parameters: $method")
    }
  }
}
