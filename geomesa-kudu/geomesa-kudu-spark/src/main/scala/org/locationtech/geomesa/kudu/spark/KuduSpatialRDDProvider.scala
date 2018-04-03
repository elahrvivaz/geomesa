/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kudu.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.spark.{DataStoreConnector, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature

class KuduSpatialRDDProvider extends SpatialRDDProvider {
  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean = ???

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    val ds = DataStoreConnector.loadingMap.get(dsParams).asInstanceOf[HBaseDataStore]
    // force loose bbox to be false
    origQuery.getHints.put(QueryHints.LOOSE_BBOX, false)

    // get the query plan to set up the iterators, ranges, etc
    lazy val sft = ds.getSchema(origQuery.getTypeName)
    lazy val qp = ds.getQueryPlan(origQuery).head

    if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
      val transform = origQuery.getHints.getTransformSchema
      SpatialRDD(sc.emptyRDD[SimpleFeature], transform.getOrElse(sft))
    } else {
      val transform = ds.queryPlanner.configureQuery(sft, origQuery).getHints.getTransformSchema
      GeoMesaConfigurator.setSchema(conf, sft)
      GeoMesaConfigurator.setSerialization(conf)
      GeoMesaConfigurator.setIndexIn(conf, qp.filter.index)
      GeoMesaConfigurator.setTable(conf, qp.table.getNameAsString)
      transform.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))
      qp.filter.secondary.foreach { f => GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(f)) }
      val scans = qp.ranges.map { s =>
        val scan = s
        // need to set the table name in each scan
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.table.getName)
        convertScanToString(scan)
      }
      conf.setStrings(MultiTableInputFormat.SCANS, scans: _*)

      val rdd = sc.newAPIHadoopRDD(conf, classOf[GeoMesaHBaseInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)
      SpatialRDD(rdd, transform.getOrElse(sft))
    }
  }

  override def save(rdd: RDD[SimpleFeature],
                    params: Map[String, String],
                    typeName: String): Unit = {
    val ds = DataStoreConnector.loadingMap.get(writeDataStoreParams).asInstanceOf[HBaseDataStore]
    try {
      require(ds.getSchema(writeTypeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    unsafeSave(rdd, writeDataStoreParams, writeTypeName)
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    * This method assumes that the schema exists.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  def unsafeSave(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    rdd.foreachPartition { iter =>
      val ds = DataStoreConnector.loadingMap.get(writeDataStoreParams).asInstanceOf[HBaseDataStore]
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach { rawFeature =>
          FeatureUtils.copyToWriter(featureWriter, rawFeature, useProvidedFid = true)
          featureWriter.write()
        }
      } finally {
        CloseQuietly(featureWriter)
        ds.dispose()
      }
    }
  }
}
