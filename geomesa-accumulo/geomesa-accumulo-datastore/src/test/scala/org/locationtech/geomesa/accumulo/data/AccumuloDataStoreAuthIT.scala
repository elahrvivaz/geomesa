/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.io.Serializable
import java.util.Collections

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.accumulo.util.WithAccumuloStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.{AuthorizationsProvider, DefaultAuthorizationsProvider, FilteringAuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAuthIT extends TestWithFeatureType {

  import scala.collection.JavaConverters._

  sequential

  override val spec = "name:String:index=join,dtg:Date,*geom:Point:srid=4326"

  val username = getClass.getSimpleName
  lazy val localConnector = createConnector(username, "pass")

  step {
    connector.securityOperations().createLocalUser(username, new PasswordToken("pass"))
    connector.securityOperations().changeUserAuthorizations(username, new Authorizations("user", "admin"))

    addFeatures((0 until 2).map { i =>
      val sf = new ScalaSimpleFeature(sft, i.toString)
      sf.setAttribute(0, i.toString)
      sf.setAttribute(1, s"2016-01-01T01:0$i:00.000Z")
      sf.setAttribute(2, s"POINT (45 5$i)")
      if (i == 0) {
        SecurityUtils.setFeatureVisibility(sf, "user")
      } else {
        SecurityUtils.setFeatureVisibility(sf, "admin")
      }
      sf
    })
  }

  def params(
      auths: String,
      provider: Option[AuthorizationsProvider] = None,
      forceEmpty: Boolean = false): java.util.Map[String, AnyRef] = {
    val overrides = Map(
      AccumuloDataStoreParams.ConnectorParam.key -> localConnector,
      AccumuloDataStoreParams.AuthsParam.key -> auths,
      AccumuloDataStoreParams.ForceEmptyAuthsParam.key -> forceEmpty.toString,
      org.locationtech.geomesa.security.AuthProviderParam.key -> provider.orNull
    ).filter(_._2 != null)
    (dsParams ++ overrides).asJava
  }

  "AccumuloDataStore" should {
    "configure authorizations by static auths" >> {
      WithAccumuloStore(params("user")) { ds =>
        ds must not(beNull)
        ds.config.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must
            beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.auths mustEqual new Authorizations("user")
      }
    }

    "configure authorizations by comma-delimited static auths" >> {
      WithAccumuloStore(params("user,admin,test")) { ds =>
        ds must not(beNull)
        ds.config.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must
            beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.auths mustEqual new Authorizations("user", "admin", "test")
      }
    }

    "fail when auth provider system property does not match an actual class" >> {
      System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY, "my.fake.Clas")
      try {
        DataStoreFinder.getDataStore(params("user,admin,test")) must throwAn[IllegalArgumentException]
      } finally {
        System.clearProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY)
      }
    }

    "fail when authorizations are explicitly provided, but the flag to force using authorizations is not set" >> {
      DataStoreFinder.getDataStore(params("user,admin,test", forceEmpty = true)) must
          throwAn[IllegalArgumentException]
    }

    "replace empty authorizations with the Accumulo user's full authorizations (without the override)" >> {
      WithAccumuloStore(params("")) { ds =>
        ds must not(beNull)
        ds.config.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must
            beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.auths mustEqual new Authorizations("user", "admin", "test")
      }
    }

    "use empty authorizations (with the override)" >> {
      WithAccumuloStore(params("", forceEmpty = true)) { ds =>
        ds must not(beNull)
        ds.config.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must
            beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.auths mustEqual new Authorizations()
      }
    }

    "query with a threaded auth provider against various indices" >> {
      val threadedAuths = new ThreadLocal[Seq[String]]
      val authProvider = new AuthorizationsProvider {
        override def getAuthorizations: java.util.List[String] = threadedAuths.get.asJava
        override def configure(params: java.util.Map[String, Serializable]): Unit = {}
      }

      WithAccumuloStore(params("user,admin", provider = Some(authProvider))) { ds =>
        ds must not(beNull)

        val user  = Seq("IN('0')", "name = '0'", "bbox(geom, 44, 49.1, 46, 50.1)", "bbox(geom, 44, 49.1, 46, 50.1) AND dtg DURING 2016-01-01T00:59:30.000Z/2016-01-01T01:00:30.000Z")
        val admin = Seq("IN('1')", "name = '1'", "bbox(geom, 44, 50.1, 46, 51.1)", "bbox(geom, 44, 50.1, 46, 51.1) AND dtg DURING 2016-01-01T01:00:30.000Z/2016-01-01T01:01:30.000Z")
        val both  = Seq("INCLUDE", "IN('0', '1')", "name < '2'", "bbox(geom, 44, 49.1, 46, 51.1)", "bbox(geom, 44, 49.1, 46, 51.1) AND dtg DURING 2016-01-01T00:59:30.000Z/2016-01-01T01:01:30.000Z")

        forall(user) { filter =>
          val q = new Query(sftName, ECQL.toFilter(filter))
          threadedAuths.set(Seq("user"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "0"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(Seq("admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must beEmpty
          } finally {
            threadedAuths.remove()
          }
        }

        forall(admin) { filter =>
          val q = new Query(sftName, ECQL.toFilter(filter))
          threadedAuths.set(Seq("admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "1"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(Seq("user"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must beEmpty
          } finally {
            threadedAuths.remove()
          }
        }

        forall(both) { filter =>
          val q = new Query(sftName, ECQL.toFilter(filter))
          threadedAuths.set(Seq("user"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "0"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(Seq("admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "1"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(Seq("user", "admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(2)
            results.map(_.getID).sorted mustEqual Seq("0", "1")
          } finally {
            threadedAuths.remove()
          }
        }
      }
    }

    "allow users with sufficient auths to write data" >> {
      val vis = new java.util.HashMap[String, Serializable](params("user,admin").asInstanceOf[java.util.Map[String, Serializable]])
      vis.put(AccumuloDataStoreParams.VisibilitiesParam.key, "user&admin")

      WithAccumuloStore(vis) { ds =>
        ds must not(beNull)

        val typeName = s"${sftName}_canwrite"
        val sft = SimpleFeatureTypes.createType(typeName, spec)
        ds.createSchema(sft)

        // write some data
        val fs = ds.getFeatureSource(typeName)
        val feat = new ScalaSimpleFeature(sft, "1")
        feat.setAttribute("geom", "POINT(45 55)")

        val written = fs.addFeatures(new ListFeatureCollection(sft, Collections.singletonList[SimpleFeature](feat)))
        written must not(beNull)
        written.size mustEqual 1
      }
    }
  }
}
