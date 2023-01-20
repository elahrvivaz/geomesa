package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.conf.{ClientProperty, Property}
import org.apache.accumulo.harness.TestingKdc
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl
import org.apache.accumulo.server.security.handler.{KerberosAuthenticator, KerberosAuthorizor, KerberosPermissionHandler}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.minikdc.MiniKdc

class KerberosCluster extends MiniCluster {

  private lazy val kdc = new TestingKdc()

  // copied from org.apache.accumulo.harness.MiniClusterHarness
  override protected def configure(config: MiniAccumuloConfigImpl, coreSite: Configuration): Unit = {
    logger.info("Enabling Kerberos/SASL for minicluster")

    // Turn on SASL and set the keytab/principal information
    config.setProperty(Property.INSTANCE_RPC_SASL_ENABLED, "true")
    val serverUser = kdc.getAccumuloServerUser

    config.setProperty(Property.GENERAL_KERBEROS_KEYTAB, serverUser.getKeytab.getAbsolutePath)
    config.setProperty(Property.GENERAL_KERBEROS_PRINCIPAL, serverUser.getPrincipal)
    config.setProperty(Property.INSTANCE_SECURITY_AUTHENTICATOR, classOf[KerberosAuthenticator].getName)
    config.setProperty(Property.INSTANCE_SECURITY_AUTHORIZOR, classOf[KerberosAuthorizor].getName)
    config.setProperty(Property.INSTANCE_SECURITY_PERMISSION_HANDLER, classOf[KerberosPermissionHandler].getName)

    // Pass down some KRB5 debug properties
    val systemProperties = config.getSystemProperties
    systemProperties.put(MiniKdc.JAVA_SECURITY_KRB5_CONF, System.getProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF, ""))
    systemProperties.put(MiniKdc.SUN_SECURITY_KRB5_DEBUG, System.getProperty(MiniKdc.SUN_SECURITY_KRB5_DEBUG, "false"))
    config.setSystemProperties(systemProperties)

    // Make sure UserGroupInformation will do the correct login
    coreSite.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")

    config.setRootUserName(kdc.getRootUser.getPrincipal)

    config.setClientProperty(ClientProperty.SASL_ENABLED, "true")
  }
}
