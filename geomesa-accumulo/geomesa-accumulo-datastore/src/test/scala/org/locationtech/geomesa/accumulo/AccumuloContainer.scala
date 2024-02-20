/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.slf4j.LoggerFactory
import org.testcontainers.containers.Container.ExecResult
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.{OutputFrame, Slf4jLogConsumer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.nio.charset.StandardCharsets
import java.time.Duration

class AccumuloContainer extends GenericContainer[AccumuloContainer](AccumuloContainer.ImageName) {

  withExposedPorts(2181, 9995, 9997)
  withLogConsumer(AccumuloContainer.LogConsumer)
  waitingFor(Wait.forLogMessage(".*Running accumulo complete.*\\n", 1).withStartupTimeout(Duration.ofMinutes(10)))


  def shell(command: String): ExecResult =
    this.execInContainer(
      s"""accumulo shell -u root -p secret -zi uno -zh localhost -e '${command.replaceAll("'", "'''")}'""")
}

object AccumuloContainer {

  val ImageName =
    DockerImageName.parse("docker-art.ccri.com/internal-utils/accumulo-uno-docker")
        .withTag(sys.props.getOrElse("accumulo.docker.tag", "0.0.6"))

  private val LogConsumer = new Slf4jLogConsumer(LoggerFactory.getLogger("accumulo"), true) {
    // suppress output once accumulo starts up, it's fairly noisy
    private var output = true
    override def accept(outputFrame: OutputFrame ): Unit = {
      if (output) {
        super.accept(outputFrame);
        if (outputFrame.getUtf8StringWithoutLineEnding.matches(".*Running accumulo complete.*")) {
          output = false
          val msg = "Container started - suppressing further output".getBytes(StandardCharsets.UTF_8)
          super.accept(new OutputFrame(OutputFrame.OutputType.STDOUT, msg))
        }
      }
    }
  }
}
