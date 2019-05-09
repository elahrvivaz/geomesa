/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.awt.RenderingHints
import java.io.Serializable
import java.nio.file.Files

import com.google.common.collect.ImmutableMap
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, ParamsAuditProvider}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore.AccumuloDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory._
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityParams}
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.io.PathUtils

import scala.collection.JavaConversions._

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): AccumuloDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): AccumuloDataStore = {
    if (MockParam.lookup(params)) { AccumuloDataStoreFactory.createMockStore(params) } else {
      val connector = ConnectorParam.lookupOpt(params).getOrElse(AccumuloDataStoreFactory.buildConnector(params))
      val config = AccumuloDataStoreFactory.buildConfig(connector, params)
      val ds = new AccumuloDataStore(connector, config)
      GeoMesaDataStore.initRemoteVersion(ds)
      ds
    }
  }

  override def isAvailable: Boolean = true

  override def getDisplayName: String = AccumuloDataStoreFactory.DisplayName

  override def getDescription: String = AccumuloDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    AccumuloDataStoreFactory.ParameterInfo ++ Array(NamespaceParam, DeprecatedGeoServerPasswordParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object AccumuloDataStoreFactory extends GeoMesaDataStoreInfo {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  override val DisplayName = "Accumulo (GeoMesa)"
  override val Description = "Apache Accumulo\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      InstanceIdParam,
      ZookeepersParam,
      CatalogParam,
      UserParam,
      PasswordParam,
      KeytabPathParam,
      AuthsParam,
      VisibilitiesParam,
      QueryTimeoutParam,
      QueryThreadsParam,
      RecordThreadsParam,
      WriteThreadsParam,
      LooseBBoxParam,
      GenerateStatsParam,
      AuditQueriesParam,
      CachingParam,
      ForceEmptyAuthsParam
    )

  @deprecated("Use DisplayName")
  val DISPLAY_NAME: String = DisplayName
  @deprecated("Use Description")
  val DESCRIPTION: String = Description

  override def canProcess(params: java.util.Map[String, _ <: Serializable]): Boolean = {
    lazy val hasInstance = Seq(InstanceIdParam, ZookeepersParam, UserParam).forall(_.exists(params))
    lazy val hasPassword = PasswordParam.exists(params) && !KeytabPathParam.exists(params)
    lazy val hasKeytab = !PasswordParam.exists(params) && KeytabPathParam.exists(params) && isKerberosAvailable
    ConnectorParam.exists(params) || (hasInstance && (hasPassword || hasKeytab))
  }

  def buildConnector(params: java.util.Map[String, Serializable]): Connector = {
    val zookeepers = ZookeepersParam.lookup(params)
    val instance = InstanceIdParam.lookup(params)
    val user = UserParam.lookup(params)
    val password = PasswordParam.lookup(params)
    val keytabPath = KeytabPathParam.lookup(params)

    val conf = new ClientConfiguration().withInstance(instance).withZkHosts(zookeepers)

    // NB: For those wanting to set this via JAVA_OPTS, this key is "instance.zookeeper.timeout" in Accumulo 1.6.x.
    SystemProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey).option.foreach { timeout =>
      conf.setProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey, timeout)
    }

    // Build authentication token according to how we are authenticating
    val token : AuthenticationToken = if (password != null && keytabPath == null) {
      new PasswordToken(password.getBytes("UTF-8"))
    } else if (password == null && keytabPath != null) {
      // explicitly enable SASL. this shouldn't be required if Accumulo client.conf is set appropriately,
      // but it doesn't seem to work
      conf.setProperty(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey, "true")
      AccumuloVersion.createKerberosToken(user, keytabPath)
    } else {
      // Should never reach here thanks to canProcess
      throw new IllegalArgumentException(s"Exactly one of '${PasswordParam.key}' or '${KeytabPathParam.key}' must be set")
    }

    new ZooKeeperInstance(conf).getConnector(user, token)
  }

  @deprecated("mock is no longer supported in Accumulo")
  def buildAccumuloConnector(params: java.util.Map[String, Serializable], useMock: Boolean): Connector = {
    if (useMock) {
      throw new IllegalArgumentException("Mock Accumulo is no longer supported - migrate to MiniAccumulo")
    }
    buildConnector(params)
  }

  def buildConfig(connector: Connector, params: java.util.Map[String, Serializable]): AccumuloDataStoreConfig = {
    val catalog = CatalogParam.lookup(params)

    val authProvider = buildAuthsProvider(connector, params)
    val auditProvider = buildAuditProvider(params)

    val auditQueries = AuditQueriesParam.lookup(params).booleanValue()
    val auditService = new AccumuloAuditService(connector, authProvider, s"${catalog}_queries", auditQueries)

    val generateStats = GenerateStatsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis)
    val visibility = VisibilitiesParam.lookupOpt(params).getOrElse("")

    val ns = NamespaceParam.lookupOpt(params)

    AccumuloDataStoreConfig(
      catalog,
      visibility,
      generateStats,
      authProvider,
      Some(auditService, auditProvider, AccumuloAuditService.StoreType),
      queryTimeout,
      LooseBBoxParam.lookup(params),
      CachingParam.lookup(params),
      WriteThreadsParam.lookup(params),
      QueryThreadsParam.lookup(params),
      RecordThreadsParam.lookup(params),
      ns
    )
  }

  def buildAuditProvider(params: java.util.Map[String, Serializable]): AuditProvider = {
    Option(AuditProvider.Loader.load(params)).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }
  }

  def buildAuthsProvider(connector: Connector, params: java.util.Map[String, Serializable]): AuthorizationsProvider = {
    val forceEmptyOpt: Option[java.lang.Boolean] = ForceEmptyAuthsParam.lookupOpt(params)
    val forceEmptyAuths = forceEmptyOpt.getOrElse(java.lang.Boolean.FALSE).asInstanceOf[Boolean]

    // convert the connector authorizations into a string array - this is the maximum auths this connector can support
    val securityOps = connector.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(connector.whoami)
    val masterAuthsStrings = masterAuths.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = AuthsParam.lookupOpt(params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    val invalidAuths = configuredAuths.filterNot(masterAuthsStrings.contains)
    if (invalidAuths.nonEmpty) {
      throw new IllegalArgumentException(s"The authorizations '${invalidAuths.mkString(",")}' " +
        "are not valid for the Accumulo connection being used")
    }

    // if the caller provided any non-null string for authorizations, use it;
    // otherwise, grab all authorizations to which the Accumulo user is entitled
    if (configuredAuths.length != 0 && forceEmptyAuths) {
      throw new IllegalArgumentException("Forcing empty auths is checked, but explicit auths are provided")
    }
    val auths: List[String] =
      if (forceEmptyAuths || configuredAuths.length > 0) { configuredAuths.toList } else { masterAuthsStrings.toList }

    AuthorizationsProvider.apply(params, auths)
  }

  // Kerberos is only available in Accumulo >= 1.7.
  // Note: doesn't confirm whether correctly configured for Kerberos e.g. core-site.xml on CLASSPATH
  def isKerberosAvailable: Boolean =
    AccumuloVersion.accumuloVersion != AccumuloVersion.V15 && AccumuloVersion.accumuloVersion != AccumuloVersion.V16

  /**
    * Spins up an accumulo mini cluster, then tears it down on 'dispose'. Note that mini cluster is 'provided'
    * scope, so won't be available on the classpath by default
    *
    * @param params data store params
    * @return
    */
  private def createMockStore(params: java.util.Map[String, Serializable]): AccumuloDataStore = {
    if (ConnectorParam.exists(params)) {
      throw new IllegalArgumentException("Can't use mock Accumulo with a provided connector")
    } else if (!PasswordParam.exists(params)) {
      throw new IllegalArgumentException(s"Parameter '${PasswordParam.key}' is required to use mock Accumulo")
    }

    val user = UserParam.lookup(params)
    val password = PasswordParam.lookup(params)

    val dir = Files.createTempDirectory("gm-accumulo-mock").toFile
    val cluster = new MiniAccumuloCluster(dir, password)
    cluster.start()

    if (user != "root") {
      cluster.getConnector("root", password).securityOperations().createLocalUser(user, new PasswordToken(password))
    }

    val updated = new java.util.HashMap[String, Serializable](params)
    updated.put(ZookeepersParam.key, cluster.getZooKeepers)
    updated.put(InstanceIdParam.key, cluster.getInstanceName)

    val connector = buildConnector(updated)
    val config = buildConfig(connector, params)

    new AccumuloDataStore(connector, config) {
      override def dispose(): Unit = {
        try { super.dispose() } finally {
          cluster.stop()
          PathUtils.deleteRecursively(dir.toPath)
        }
      }
    }
  }
}

// keep params in a separate object so we don't require accumulo classes on the build path to access it
object AccumuloDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {
  val ConnectorParam       = new GeoMesaParam[Connector]("accumulo.connector", "Accumulo connector", deprecatedKeys = Seq("connector"))
  val InstanceIdParam      = new GeoMesaParam[String]("accumulo.instance.id", "Accumulo Instance ID", optional = false, deprecatedKeys = Seq("instanceId", "accumulo.instanceId"))
  val ZookeepersParam      = new GeoMesaParam[String]("accumulo.zookeepers", "Zookeepers", optional = false, deprecatedKeys = Seq("zookeepers"))
  val UserParam            = new GeoMesaParam[String]("accumulo.user", "Accumulo user", optional = false, deprecatedKeys = Seq("user"))
  val PasswordParam        = new GeoMesaParam[String]("accumulo.password", "Accumulo password", password = true, deprecatedKeys = Seq("password"))
  val KeytabPathParam      = new GeoMesaParam[String]("accumulo.keytab.path", "Path to keytab file", deprecatedKeys = Seq("keytabPath", "accumulo.keytabPath"))
  val MockParam            = new GeoMesaParam[java.lang.Boolean]("accumulo.mock", "Use a mock connection (for testing)", default = false, deprecatedKeys = Seq("useMock", "accumulo.useMock"))
  val CatalogParam         = new GeoMesaParam[String]("accumulo.catalog", "Accumulo catalog table name", optional = false, deprecatedKeys = Seq("tableName", "accumulo.tableName"))
  val RecordThreadsParam   = new GeoMesaParam[Integer]("accumulo.query.record-threads", "The number of threads to use for record retrieval", default = 10, deprecatedKeys = Seq("recordThreads", "accumulo.recordThreads"))
  val WriteThreadsParam    = new GeoMesaParam[Integer]("accumulo.write.threads", "The number of threads to use for writing records", default = 10, deprecatedKeys = Seq("writeThreads", "accumulo.writeThreads"))

  // used to handle geoserver password encryption in persisted ds params
  val DeprecatedGeoServerPasswordParam = new Param("password", classOf[String], "", false, null, ImmutableMap.of(Parameter.DEPRECATED, true, Parameter.IS_PASSWORD, true))
}