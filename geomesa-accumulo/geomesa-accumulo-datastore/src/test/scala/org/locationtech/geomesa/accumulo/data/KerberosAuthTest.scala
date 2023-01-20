/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.security.UserGroupInformation
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.KerberosCluster
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class KerberosAuthTest extends Specification {

  import scala.collection.JavaConverters._

  private val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")

  "AccumuloDataStore" should {
    "work with kerberos auth" in {
      val user = "test-user"
      val cluster = new KerberosCluster()
      try {
        val userKeytab = new File(cluster.kdc.getKeytabDir, s"$user.keytab")
        if (userKeytab.exists && !userKeytab.delete) {
          ko(s"Unable to delete ${userKeytab.getAbsolutePath}")
        }
        cluster.kdc.createPrincipal(userKeytab, user)

        val qualifiedUser = cluster.kdc.qualifyUser(user)
        val catalog = s"${cluster.namespace}.${getClass.getSimpleName}"

        val params =
          Map(
            AccumuloDataStoreParams.InstanceNameParam.key -> cluster.cluster.getInstanceName,
            AccumuloDataStoreParams.ZookeepersParam.key -> cluster.cluster.getZooKeepers,
            AccumuloDataStoreParams.UserParam.key -> qualifiedUser,
            AccumuloDataStoreParams.KeytabPathParam.key -> userKeytab.getAbsolutePath,
            AccumuloDataStoreParams.CatalogParam.key -> catalog,
          )
        val conf = new Configuration(false)
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        UserGroupInformation.setConfiguration(conf)
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          ds must not(beNull)
          ds.getTypeNames must beEmpty
          ds.createSchema(sft) must throwAn[Exception] // no permissions
        }
      } finally {
        cluster.close()
      }
    }
  }
}
