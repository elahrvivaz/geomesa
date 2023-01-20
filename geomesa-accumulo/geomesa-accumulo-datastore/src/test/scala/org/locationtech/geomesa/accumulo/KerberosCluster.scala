/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.conf.{ClientProperty, Property}
import org.apache.accumulo.harness.TestingKdc
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl
import org.apache.accumulo.server.security.handler.{KerberosAuthenticator, KerberosAuthorizor, KerberosPermissionHandler}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.minikdc.MiniKdc

class KerberosCluster(namespace: String = "gm") extends MiniCluster(namespace) {

  lazy val kdc = new TestingKdc()

  // copied from org.apache.accumulo.harness.MiniClusterHarness
  override protected def configure(config: MiniAccumuloConfigImpl, coreSite: Configuration): Unit = {
    logger.info("Enabling Kerberos/SASL for minicluster")

    kdc.start()

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

  override protected def setup(cluster: MiniAccumuloCluster): Unit = {

  }
}
