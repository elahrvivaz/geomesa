/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloInstance}
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments

/**
  * Trait for tests that require an accumulo cluster. Classes extending this trait should be named 'fooIT.scala'
  * so that they are run during the integration-test phase in maven, which will ensure that a single mini cluster
  * is shared between all the tests
  */
abstract class TestWithMiniCluster extends Specification with LazyLogging {

  private var cluster: MiniAccumuloCluster = _

  lazy val connector: Connector = {
    val dir = new File(getClass.getClassLoader.getResource("accumulo-maven-plugin").toURI)
    val pass = sys.props.get("mini.accumulo.password").getOrElse("test-secret")

    // through maven, a single mini cluster will be spun up through the pre-integration-test phase
    // if the sys prop isn't set, we assume we're running through intellij and spin up a culster just for this test
    sys.props.get("mini.accumulo.instance") match {
      case Some(i) => new MiniAccumuloInstance(i, new File(dir, i)).getConnector("root", new PasswordToken(pass))
      case None =>
        logger.warn("No mini cluster detected - starting cluster just for this test")
        cluster = new MiniAccumuloCluster(dir, pass)
        cluster.getConnector("root", pass)
    }
  }

  // after all tests, shut down the mini cluster if we started it
  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
    if (cluster != null) {
      logger.info("Shutting down mini cluster")
      cluster.stop()
    }
  }
}
