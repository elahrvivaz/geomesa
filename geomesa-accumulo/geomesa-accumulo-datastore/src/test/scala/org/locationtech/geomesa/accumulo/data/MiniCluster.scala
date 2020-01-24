/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{AccumuloClient}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import java.io.File
import com.google.common.io.Files

case object MiniCluster extends LazyLogging {

  lazy val cluster: MiniAccumuloCluster = {

    logger.info("Starting accumulo minicluster")
    val miniClusterTempDir: File = Files.createTempDir();
    val cluster = new MiniAccumuloCluster( miniClusterTempDir, "admin")

    cluster.start
    logger.info("Started accumulo minicluster")
    cluster
  }

  lazy val client: AccumuloClient =  cluster.createAccumuloClient("root", new PasswordToken("admin"))

  sys.addShutdownHook({
    logger.info("Stopping accumulo minicluster")
    // note: HBaseTestingUtility says don't close the connection
    // connection.close()
    cluster.stop
    logger.info("Accumulo minicluster stopped")
  })
}