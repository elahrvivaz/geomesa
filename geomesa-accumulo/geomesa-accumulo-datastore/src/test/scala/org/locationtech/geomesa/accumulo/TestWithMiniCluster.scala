/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import java.io.File
import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloInstance}
import org.locationtech.geomesa.utils.io.PathUtils
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments

/**
  * Trait for tests that require an accumulo cluster. Classes extending this trait should be named 'fooIT.scala'
  * so that they are run during the integration-test phase in maven, which will ensure that a single mini cluster
  * is shared between all the tests
  */
abstract class TestWithMiniCluster extends Specification with LazyLogging {

  lazy val password = sys.props.get("mini.accumulo.password").getOrElse("test-secret")

  // through maven, a single mini cluster will be spun up through the pre-integration-test phase
  // if the sys prop isn't set, we assume we're running through intellij and spin up a cluster just for this test
  private lazy val instance: Either[MiniAccumuloCluster, MiniAccumuloInstance] = {
    val target = new File("target/accumulo-maven-plugin")
    sys.props.get("mini.accumulo.instance") match {
      case Some(i) if target.exists =>
        Right(new MiniAccumuloInstance(i, new File(target, i)))

      case _ =>
        logger.warn("No mini cluster detected - starting cluster just for this test")
        val dir = Files.createTempDirectory("accumulo-maven-plugin").toFile
        val cluster = new MiniAccumuloCluster(dir, password)
        cluster.start()
        Left(cluster)
    }
  }

  /**
    * Root connector
    */
  lazy val connector: Connector = createConnector("root", password)

  lazy val zookeepers = instance match {
    case Right(i) => i.getZooKeepers
    case Left(c)  => c.getZooKeepers
  }

  lazy val instanceId = instance match {
    case Right(i) => i.getInstanceName
    case Left(c)  => c.getInstanceName
  }

  /**
    * Create a new connector to the mini accumulo instance
    *
    * @param user user (must exist)
    * @param password password
    * @return
    */
  def createConnector(user: String, password: String): Connector = {
    instance match {
      case Right(i) => i.getConnector(user, new PasswordToken(password))
      case Left(c)  => c.getConnector(user, password)
    }
  }

  // after all tests, shut down the mini cluster if we started it
  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
    instance.left.foreach { cluster =>
      logger.info("Shutting down mini cluster")
      try { cluster.stop() } finally {
        PathUtils.deleteRecursively(cluster.getConfig.getDir.toPath)
      }
    }
  }
}
