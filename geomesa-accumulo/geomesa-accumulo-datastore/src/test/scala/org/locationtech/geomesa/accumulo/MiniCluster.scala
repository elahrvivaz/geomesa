/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.security.{Authorizations, NamespacePermission, SystemPermission}
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloConfig}
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import java.io._
import java.nio.file.Files
import scala.collection.JavaConverters._

case object MiniCluster {

  val namespace = "gm"

  lazy private val mc = new MiniCluster(namespace)

  lazy val cluster = mc.cluster

  private val systemPermissions = Seq(
    SystemPermission.CREATE_NAMESPACE,
    SystemPermission.ALTER_NAMESPACE,
    SystemPermission.DROP_NAMESPACE
  )

  private val namespacePermissions = Seq(
    NamespacePermission.READ,
    NamespacePermission.WRITE,
    NamespacePermission.CREATE_TABLE,
    NamespacePermission.ALTER_TABLE,
    NamespacePermission.DROP_TABLE
  )

  sys.addShutdownHook(mc.close())

  case class UserWithAuths(name: String, password: String, auths: Authorizations)

  object Users {
    val root  = UserWithAuths("root", "secret", new Authorizations("admin", "user", "system"))
    val admin = UserWithAuths("admin", "secret", new Authorizations("admin", "user"))
    val user  = UserWithAuths("user", "secret", new Authorizations("user"))
  }
}

class MiniCluster(val namespace: String = "gm") extends Closeable with LazyLogging {

  import MiniCluster._

  private val miniClusterTempDir = Files.createTempDirectory("gm-mini-acc-")

  val cluster: MiniAccumuloCluster = {
    logger.info(s"Starting Accumulo minicluster at $miniClusterTempDir")

    val config = new MiniAccumuloConfig(miniClusterTempDir.toFile, Users.root.password)
    sys.props.get("geomesa.accumulo.test.tablet.servers").map(_.toInt).foreach(config.setNumTservers)
    config.setDefaultMemory(256, org.apache.accumulo.minicluster.MemoryUnit.MEGABYTE) // default is 128MB

    // Use reflection to access a package-private method to set system properties before starting
    // the minicluster. It is possible that this could break with future versions of Accumulo.
    val configGetImpl = config.getClass.getDeclaredMethod("getImpl")
    val systemProps =
      Map.empty[String, String] ++
          Option("zookeeper.jmx.log4j.disable").flatMap(key => sys.props.get(key).map(key -> _))

    configGetImpl.setAccessible(true)
    val configImpl = configGetImpl.invoke(config).asInstanceOf[MiniAccumuloConfigImpl]
    configImpl.setSystemProperties(systemProps.asJava)
    configImpl.setProperty(Property.INSTANCE_ZK_TIMEOUT.getKey, "15s")

    val coreSite = new Configuration(false)
    configure(configImpl, coreSite)

    val cluster = new MiniAccumuloCluster(config) // required for zookeeper 3.5
    WithClose(new FileWriter(new File(configImpl.getConfDir, "zoo.cfg"), true)) { writer =>
      writer.write("admin.enableServer=false\n") // disable the admin server, which tries to bind to 8080
      writer.write("4lw.commands.whitelist=*\n") // enable 'ruok', which the minicluster uses to check zk status
    }

    // Write out any configuration items to a file so HDFS will pick them up automatically (from the classpath)
    if (coreSite.size > 0) {
      val csFile = new File(configImpl.getConfDir, "core-site.xml")
      WithClose(new BufferedOutputStream(new FileOutputStream(csFile)))(coreSite.writeXml)
    }

    cluster.start()

    setup(cluster)

    logger.info("Started Accumulo minicluster")

    cluster
  }

  protected def configure(config: MiniAccumuloConfigImpl, coreSite: Configuration): Unit = {}

  protected def setup(cluster: MiniAccumuloCluster): Unit = { // set up users and authorizations
    WithClose(cluster.createAccumuloClient(Users.root.name, new PasswordToken(Users.root.password))) { connector =>
      connector.namespaceOperations().create(namespace)
      Seq(Users.root, Users.admin, Users.user).foreach { case UserWithAuths(name, password, auths) =>
        if (name != Users.root.name) {
          connector.securityOperations().createLocalUser(name, new PasswordToken(password))
          systemPermissions.foreach(p => connector.securityOperations().grantSystemPermission(name, p))
          namespacePermissions.foreach(p => connector.securityOperations().grantNamespacePermission(name, namespace, p))
        }
        connector.securityOperations().changeUserAuthorizations(name, auths)
      }
    }
  }

  override def close(): Unit = {
    logger.info("Stopping Accumulo minicluster")
    try { cluster.stop() } finally {
      PathUtils.deleteRecursively(miniClusterTempDir)
    }
    logger.info("Stopped Accumulo minicluster")
  }
}
