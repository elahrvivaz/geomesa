/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.ingest

import java.io.File

import org.apache.hadoop.hbase.client.Connection
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.tools.utils.LibJarsLoader
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

trait HBaseLibJars extends LibJarsLoader {

  // TODO need to pass hbase-site.xml around?
  override val libjarsFile: String = "org/locationtech/geomesa/hbase/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HBASE_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("HBASE_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[HBaseDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[Connection])
  )
}
