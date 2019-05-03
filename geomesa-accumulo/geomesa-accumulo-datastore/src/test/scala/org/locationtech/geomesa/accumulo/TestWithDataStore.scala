/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.specification.core.Fragments

import scala.collection.JavaConverters._

/**
  * Trait to simplify tests that require reading and writing features from an AccumuloDataStore.
  *
  * Classes extending this trait should be named 'fooIT.scala' so that they are run during the
  * integration-test phase in maven, which will ensure that a single mini cluster is shared between all the tests
  */
abstract class TestWithDataStore extends TestWithMiniCluster {

  def additionalDsParams(): Map[String, Any] = Map.empty

  // we use class name to prevent spillage between unit tests in the mock connector
  lazy val catalog = getClass.getSimpleName

//  val EmptyUserAuthorizations = new Authorizations()
//  val EmptyUserAuthSeq = Seq.empty[String]
//
//  val MockUserAuthorizationsString = "A,B,C"
//  val MockUserAuthorizations = new Authorizations(
//    MockUserAuthorizationsString.split(",").map(_.getBytes()).toList.asJava
//  )
//  val MockUserAuthSeq = Seq("A", "B", "C")
//
//  lazy val mockInstanceId = "mycloud"
//  lazy val mockZookeepers = "myzoo"
//  lazy val mockUser = "user"
//  lazy val mockPassword = "password"
//
//
//  lazy val mockInstance = new MockInstance(mockInstanceId)
//
//  // assign some default authorizations to this mock user
//  lazy val connector: Connector = {
//    val mockConnector = mockInstance.getConnector(mockUser, new PasswordToken(mockPassword))
//    mockConnector.securityOperations().changeUserAuthorizations(mockUser, MockUserAuthorizations)
//    mockConnector
//  }

  lazy val dsParams = Map(
    AccumuloDataStoreParams.ConnectorParam.key -> connector,
    AccumuloDataStoreParams.CachingParam.key   -> false,
    // note the table needs to be different to prevent testing errors
    AccumuloDataStoreParams.CatalogParam.key   -> catalog
  ) ++ additionalDsParams()

  lazy val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]

  // after all tests, drop the tables we created to free up memory
  override def map(fragments: => Fragments): Fragments = super.map(fragments ^ fragmentFactory.step(ds.dispose()))

  /**
   * Call to load the test features into the data store
   */
  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    val sft = features.head.getFeatureType.getTypeName
    WithClose(ds.getFeatureWriterAppend(sft, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach { f =>
        FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
        writer.write()
      }
    }
  }

  def clearFeatures(typeName: String): Unit = ds.getFeatureSource(typeName).removeFeatures(Filter.INCLUDE)

  def explain(query: Query): String = {
    val o = new ExplainString
    ds.getQueryPlan(query, explainer = o)
    o.toString()
  }
}
