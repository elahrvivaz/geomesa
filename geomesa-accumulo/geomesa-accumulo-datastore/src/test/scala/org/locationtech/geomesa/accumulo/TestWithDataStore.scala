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
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
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

  def additionalDsParams(): Map[String, AnyRef] = Map.empty

  // we use class name to prevent spillage between unit tests in the mock connector
  lazy val catalog = getClass.getSimpleName

  lazy val dsParams = Map(
    AccumuloDataStoreParams.ConnectorParam.key -> connector,
    AccumuloDataStoreParams.CachingParam.key   -> Boolean.box(false),
    // note the table needs to be different to prevent testing errors
    AccumuloDataStoreParams.CatalogParam.key   -> catalog
  ) ++ additionalDsParams()

  lazy val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]

  // after all tests, drop the tables we created to free up memory
  override def map(fragments: => Fragments): Fragments = super.map(fragments ^ fragmentFactory.step(ds.dispose()))

  /**
    * Ingest a single feature to the data store
    *
    * @param feature feature to ingest
    */
  def addFeature(feature: SimpleFeature): Unit = addFeatures(Seq(feature))

  /**
    * Ingest features to the data store
    *
    * @param features features to ingest (non-empty)
    */
  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    val sft = features.headOption.map(_.getFeatureType.getTypeName).getOrElse {
      throw new IllegalArgumentException("Trying to add an empty feature collection")
    }
    WithClose(ds.getFeatureWriterAppend(sft, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach { f =>
        FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
        writer.write()
      }
    }
  }

  def clearFeatures(typeName: String): Unit = ds.getFeatureSource(typeName).removeFeatures(Filter.INCLUDE)

  def runQuery(query: Query): Seq[SimpleFeature] =
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

  def explain(query: Query): String = {
    val o = new ExplainString
    ds.getQueryPlan(query, explainer = o)
    o.toString()
  }
}
