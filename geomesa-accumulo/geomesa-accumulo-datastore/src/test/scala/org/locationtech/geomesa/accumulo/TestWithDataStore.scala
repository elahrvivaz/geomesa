/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.client.AccumuloClient
import org.locationtech.geomesa.accumulo.data.MiniCluster
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments

import scala.collection.JavaConverters._

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithDataStore extends Specification {

  def spec: String
  def dtgField: Option[String] = Some("dtg")

  def additionalDsParams(): Map[String, Any] = Map.empty

  // we use class name to prevent spillage between unit tests in the MiniCluster
  lazy val sftName: String = getClass.getSimpleName

  val EmptyUserAuthorizations = new Authorizations()
  val EmptyUserAuthSeq = Seq.empty[String]

  val MockUserAuthorizationsString = "A,B,C"
  val MockUserAuthorizations = new Authorizations(
    MockUserAuthorizationsString.split(",").map(_.getBytes()).toList.asJava
  )
  val MockUserAuthSeq = Seq("A", "B", "C")

  lazy val mockInstanceId = "mycloud"
  lazy val mockZookeepers = "myzoo"
  lazy val mockUser = "user"
  lazy val mockPassword = "password"

  lazy val catalog = sftName

  

  // assign some default authorizations to this mock user
  lazy val client: AccumuloClient = {
    val miniCluster = MiniCluster.client
    miniCluster.securityOperations().changeUserAuthorizations(mockUser, MockUserAuthorizations)
    miniCluster
  }

  lazy val dsParams = Map(
    AccumuloDataStoreParams.ConnectorParam.key -> client,
    AccumuloDataStoreParams.CachingParam.key   -> false,
    // note the table needs to be different to prevent testing errors
    AccumuloDataStoreParams.CatalogParam.key   -> catalog
  ) ++ additionalDsParams()

  lazy val (ds, sft) = {
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    dtgField.foreach(sft.setDtgField)
    val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    (ds, ds.getSchema(sftName)) // reload the sft from the ds to ensure all user data is set properly
  }

  lazy val fs = ds.getFeatureSource(sftName)

  // after all tests, drop the tables we created to free up memory
  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
    ds.removeSchema(sftName)
    ds.dispose()
  }

  /**
   * Call to load the test features into the data store
   */
  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  def clearFeatures(): Unit = {
    val writer = ds.getFeatureWriter(sftName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
    while (writer.hasNext) {
      writer.next()
      writer.remove()
    }
    writer.close()
  }

  def explain(query: Query): String = {
    val o = new ExplainString
    ds.getQueryPlan(query, explainer = o)
    o.toString()
  }

  def explain(filter: String): String = explain(new Query(sftName, ECQL.toFilter(filter)))

  def rowToString(key: Key) = bytesToString(key.getRow.copyBytes())

  def bytesToString(bytes: Array[Byte]) = Key.toPrintableString(bytes, 0, bytes.length, bytes.length)
}
