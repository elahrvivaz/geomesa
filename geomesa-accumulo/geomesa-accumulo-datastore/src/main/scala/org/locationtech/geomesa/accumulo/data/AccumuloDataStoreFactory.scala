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
import java.util.{Map => JMap, Properties}

import com.google.common.collect.ImmutableMap
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, KerberosToken, PasswordToken}
import org.apache.accumulo.core.client.{ClientConfiguration, ZooKeeperInstance, Accumulo, AccumuloClient}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, ParamsAuditProvider}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore.AccumuloDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory._
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityParams}
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import scala.collection.JavaConversions._

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: JMap[String, Serializable]): AccumuloDataStore = createDataStore(params)

  override def createDataStore(params: JMap[String, Serializable]): AccumuloDataStore = {
    val client : AccumuloClient = ConnectorParam.lookupOpt(params).getOrElse {
      buildAccumuloClient(params, MockParam.lookup(params))
    }
    val config = buildConfig(client, params)
    val ds = new AccumuloDataStore(client, config)
    GeoMesaDataStore.initRemoteVersion(ds)
    ds
  }

  override def isAvailable = true

  override def getDisplayName: String = AccumuloDataStoreFactory.DISPLAY_NAME

  override def getDescription: String = AccumuloDataStoreFactory.DESCRIPTION

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
    val hasConnector = ConnectorParam.lookupOpt(params).isDefined
    def hasConnection = InstanceIdParam.exists(params) && ZookeepersParam.exists(params) && UserParam.exists(params)
    def hasMock = InstanceIdParam.exists(params) && UserParam.exists(params) && MockParam.lookupOpt(params).contains(java.lang.Boolean.TRUE)
    def hasPassword = PasswordParam.exists(params) && !KeytabPathParam.exists(params)
    def hasKeytab = !PasswordParam.exists(params) && KeytabPathParam.exists(params) && isKerberosAvailable
    hasConnector || ((hasConnection || hasMock) && (hasPassword || hasKeytab))
  }

  def buildAccumuloClient(params: JMap[String,Serializable], useMock: Boolean): AccumuloClient = {
    if(useMock){
      throw new IllegalArgumentException("useMock = true has been removed.")
    }else{
      val instance = InstanceIdParam.lookup(params)
      val user = UserParam.lookup(params)
      val password = PasswordParam.lookup(params)
      val keytabPath = KeytabPathParam.lookup(params)
      val zookeepers = ZookeepersParam.lookup(params)
      // NB: For those wanting to set this via JAVA_OPTS, this key is "instance.zookeeper.timeout" in Accumulo 1.6.x.
      val timeout = GeoMesaSystemProperties.getProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey)
      val props = new Properties()
      // Build authentication token according to how we are authenticating
      val authTokenTypeTuple : (String, String) = if (password != null && keytabPath == null) {
        ("org.apache.accumulo.core.client.security.tokens.PasswordToken", password)
      } else if (password == null && keytabPath != null) {
          // This API is only in Accumulo >=1.7, but canProcess should ensure this isn't actually invoked on earlier
          ("kerberos", (new KerberosToken(user, new java.io.File(keytabPath))).toString)
      } else {
        // Should never reach here thanks to canProcess
        throw new IllegalArgumentException("Neither or both of password & keytabPath are set")
      }


      val (tokenType, token) = authTokenTypeTuple

      props.put("auth.type", tokenType)
      props.put("auth.principal", user)
      props.put("auth.token", token)

      System.out.println(params)
      System.out.println("password: " + password)
      System.out.println("props:"+  props)


      
      props.put("instance.name", instance)
      props.put("instance.zookeepers", zookeepers)
      if (timeout !=  null) props.put(ClientProperty.INSTANCE_ZK_TIMEOUT, timeout)
      
      Accumulo.newClient().from(props).build()
    }
  }

  def buildConfig(client: AccumuloClient, params: JMap[String, Serializable]): AccumuloDataStoreConfig = {
    val catalog = CatalogParam.lookup(params)

    val authProvider = buildAuthsProvider(client, params)
    val auditProvider = buildAuditProvider(params)

    // if explicit, use param, else if mocked, false, else use default
    val auditQueries: Boolean = if (AuditQueriesParam.exists(params)) { 
      AuditQueriesParam.lookup(params).booleanValue() 
      } else {
        AuditQueriesParam.default
      }
    val auditService = new AccumuloAuditService(client, authProvider, s"${catalog}_queries", auditQueries)

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

  def buildAuditProvider(params: JMap[String, Serializable]): AuditProvider = {
    Option(AuditProvider.Loader.load(params)).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }
  }

  def buildAuthsProvider(client: AccumuloClient, params: JMap[String, Serializable]): AuthorizationsProvider = {
    val forceEmptyOpt: Option[java.lang.Boolean] = ForceEmptyAuthsParam.lookupOpt(params)
    val forceEmptyAuths = forceEmptyOpt.getOrElse(java.lang.Boolean.FALSE).asInstanceOf[Boolean]

    // convert the client authorizations into a string array - this is the maximum auths this client can support
    val securityOps = client.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(client.whoami)
    val masterAuthsStrings = masterAuths.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = AuthsParam.lookupOpt(params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the client we are using (fail-fast)
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
      if (forceEmptyAuths || configuredAuths.length > 0) configuredAuths.toList
      else masterAuthsStrings.toList

    AuthorizationsProvider.apply(params, auths)
  }

  // Kerberos is only available in Accumulo >= 1.7.
  // Note: doesn't confirm whether correctly configured for Kerberos e.g. core-site.xml on CLASSPATH
  def isKerberosAvailable: Boolean =
    AccumuloVersion.accumuloVersion != AccumuloVersion.V15 && AccumuloVersion.accumuloVersion != AccumuloVersion.V16
}

// keep params in a separate object so we don't require accumulo classes on the build path to access it
object AccumuloDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {

  val ConnectorParam =
    new GeoMesaParam[AccumuloClient](
      "accumulo.connector",
      "Accumulo connector",
      deprecatedKeys = Seq("connector"))

  val InstanceIdParam =
    new GeoMesaParam[String](
      "accumulo.instance.id",
      "Accumulo Instance ID",
      optional = false,
      deprecatedKeys = Seq("instanceId", "accumulo.instanceId"),
      supportsNiFiExpressions = true)

  val ZookeepersParam =
    new GeoMesaParam[String](
      "accumulo.zookeepers",
      "Zookeepers",
      optional = false,
      deprecatedKeys = Seq("zookeepers"),
      supportsNiFiExpressions = true)

  val UserParam =
    new GeoMesaParam[String](
      "accumulo.user",
      "Accumulo user",
      optional = false,
      deprecatedKeys = Seq("user"),
      supportsNiFiExpressions = true)

  val PasswordParam =
    new GeoMesaParam[String](
      "accumulo.password",
      "Accumulo password",
      password = true,
      deprecatedKeys = Seq("password"),
      supportsNiFiExpressions = true)

  val KeytabPathParam =
    new GeoMesaParam[String](
      "accumulo.keytab.path",
      "Path to keytab file",
      deprecatedKeys = Seq("keytabPath", "accumulo.keytabPath"),
      supportsNiFiExpressions = true)

  val MockParam =
    new GeoMesaParam[java.lang.Boolean](
      "accumulo.mock",
      "Use a mock connection (for testing)",
      default = false,
      deprecatedKeys = Seq("useMock", "accumulo.useMock"))

  val CatalogParam =
    new GeoMesaParam[String](
      "accumulo.catalog",
      "Accumulo catalog table name",
      optional = false,
      deprecatedKeys = Seq("tableName", "accumulo.tableName"),
      supportsNiFiExpressions = true)

  val RecordThreadsParam =
    new GeoMesaParam[Integer](
      "accumulo.query.record-threads",
      "The number of threads to use for record retrieval",
      default = 10,
      deprecatedKeys = Seq("recordThreads", "accumulo.recordThreads"),
      supportsNiFiExpressions = true)

  val WriteThreadsParam =
    new GeoMesaParam[Integer](
      "accumulo.write.threads",
      "The number of threads to use for writing records",
      default = 10,
      deprecatedKeys = Seq("writeThreads", "accumulo.writeThreads"),
      supportsNiFiExpressions = true)

  // used to handle geoserver password encryption in persisted ds params
  val DeprecatedGeoServerPasswordParam =
    new Param("password", classOf[String], "", false, null, ImmutableMap.of(Parameter.DEPRECATED, true, Parameter.IS_PASSWORD, true))
}