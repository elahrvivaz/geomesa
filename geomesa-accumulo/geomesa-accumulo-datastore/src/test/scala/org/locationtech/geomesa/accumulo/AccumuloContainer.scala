/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import com.typesafe.scalalogging.StrictLogging
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Accumulo, AccumuloClient}
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.security.{Authorizations, NamespacePermission, SystemPermission}
import org.locationtech.geomesa.accumulo.AccumuloContainer.UserWithAuths
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.containers.output.{OutputFrame, Slf4jLogConsumer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.{DockerImageName, MountableFile}

import java.io.{File, FilenameFilter}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

class AccumuloContainer extends GenericContainer[AccumuloContainer](AccumuloContainer.ImageName) {

  import AccumuloContainer.Users

  // get random ports to avoid host port conflicts
  private val zookeeperPort = WithClose(new ServerSocket(0))(_.getLocalPort)
  private val tserverPort = WithClose(new ServerSocket(0))(_.getLocalPort)

  addExposedPorts(9995) // accumulo monitor, for debugging
  addFixedExposedPort(zookeeperPort, zookeeperPort)
  addFixedExposedPort(tserverPort, tserverPort)
  addEnv("ZOOKEEPER_PORT", s"$zookeeperPort")
  addEnv("TSERVER_PORT", s"$tserverPort")

  withCopyFileToContainer(
    MountableFile.forHostPath(AccumuloContainer.DistributedRuntimeJarPath),
    "/fluo-uno/install/accumulo/lib/geomesa-accumulo-distributed-runtime.jar")

  withLogConsumer(AccumuloContainer.LogConsumer)
  waitingFor(Wait.forLogMessage(".*Running accumulo complete.*\\n", 1).withStartupTimeout(Duration.ofMinutes(10)))

  val instanceName = "uno"
  val user = AccumuloContainer.Users.root.name
  val password = AccumuloContainer.Users.root.password

  lazy val zookeepers = s"$getHost:$zookeeperPort"

  /**
   * Gets an Accumulo Client to connect to this container
   *
   * @return
   */
  def client(user: UserWithAuths = Users.root): AccumuloClient = {
    val props = new Properties()
    props.setProperty(ClientProperty.INSTANCE_NAME.getKey, instanceName)
    props.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey, zookeepers)
    props.list(System.out)
    val password = new PasswordToken(user.password.getBytes(StandardCharsets.UTF_8))
    Accumulo.newClient().from(props).as(user.name, password).build()
  }
}

case object AccumuloContainer extends StrictLogging {

  // TODO accumulo 2.0 vs 2.1, esp for spark distributed runtime tests
  val ImageName =
    DockerImageName.parse("docker-art.ccri.com/internal-utils/accumulo-uno-docker")
        .withTag(sys.props.getOrElse("accumulo.docker.tag", "2.1"))

  val Namespace = "gm"

  lazy val Container: AccumuloContainer = {
    val container = tryContainer.get
    WithClose(container.client()) { client =>
      val secOps = client.securityOperations()
      secOps.changeUserAuthorizations(Users.root.name, Users.root.auths)
      Seq(Users.admin, Users.user).foreach { case UserWithAuths(name, password, auths) =>
        secOps.createLocalUser(name, new PasswordToken(password))
        SystemPermissions.foreach(p => secOps.grantSystemPermission(name, p))
        NamespacePermissions.foreach(p => secOps.grantNamespacePermission(name, Namespace, p))
        client.securityOperations().changeUserAuthorizations(name, auths)
      }
    }
    container
  }

  private lazy val tryContainer: Try[AccumuloContainer] = Try {
    logger.info("Starting Accumulo container")
    val container = new AccumuloContainer()
    initialized.getAndSet(true)
    container.start()
    logger.info("Started Accumulo container")
    container
  }

  private val initialized = new AtomicBoolean(false)

  sys.addShutdownHook({
    if (initialized.get) {
      logger.info("Stopping Accumulo container")
      tryContainer.foreach(_.stop())
      logger.info("Stopped Accumulo container")
    }
  })

  case class UserWithAuths(name: String, password: String, auths: Authorizations)

  object Users {
    val root  = UserWithAuths("root", "secret", new Authorizations("admin", "user", "system"))
    val admin = UserWithAuths("admin", "secret", new Authorizations("admin", "user"))
    val user  = UserWithAuths("user", "secret", new Authorizations("user"))
  }

  private val SystemPermissions = Seq(
    SystemPermission.CREATE_NAMESPACE,
    SystemPermission.ALTER_NAMESPACE,
    SystemPermission.DROP_NAMESPACE
  )

  private val NamespacePermissions = Seq(
    NamespacePermission.READ,
    NamespacePermission.WRITE,
    NamespacePermission.CREATE_TABLE,
    NamespacePermission.ALTER_TABLE,
    NamespacePermission.DROP_TABLE
  )

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

  private lazy val DistributedRuntimeJarPath: String = {
    val lookup =
      Option(getClass.getClassLoader.getResource("geomesa-accumulo-distributed-runtime.properties"))
          .map(_.toURI)
    logger.debug(s"Distributed runtime lookup: ${lookup.orNull}")
    val jar = lookup.flatMap {
      // running through intellij
      case file if file.toString.endsWith("/target/classes/geomesa-accumulo-distributed-runtime.properties") =>
        val targetDir = Paths.get(file).toFile.getParentFile.getParentFile
        val names = targetDir.listFiles(new FilenameFilter() {
          override def accept(dir: File, name: String): Boolean = {
            name.startsWith("geomesa-accumulo-distributed-runtime_") &&
                name.endsWith("-SNAPSHOT.jar") || name.matches(".*-[0-9]+\\.[0-9]+\\.[0-9]+\\.jar")
          }
        })
        names match {
          case Array(jar) => Some(jar.getAbsolutePath)
          case _ =>
            logger.debug(s"Distributed runtime jars: ${names.mkString(", ")}")
            None
        }

      // running through maven
      case file if file.toString.contains(".jar!") =>
        Some(Paths.get(file.toString.replaceAll("\\.jar!.*", ".jar")).toFile.getAbsolutePath)

      case _ =>
        None
    }

    jar.getOrElse(throw new RuntimeException("Could not load geomesa-accumulo-distributed-runtime JAR from classpath"))
  }
}
