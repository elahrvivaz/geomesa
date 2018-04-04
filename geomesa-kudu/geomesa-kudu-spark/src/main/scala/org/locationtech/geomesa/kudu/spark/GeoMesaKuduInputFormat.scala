/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kudu.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.kudu.mapreduce.{KuduTableInputFormat, KuduTableMapReduceUtil}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.hbase.data.HBaseConnectionPool
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.hbase.jobs.HBaseGeoMesaRecordReader
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.kudu.data.{KuduDataStore, KuduQueryPlan}
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.locationtech.geomesa.kudu.schema.KuduResultAdapter
import org.locationtech.geomesa.kudu.schema.KuduResultAdapter.ResultAdapter
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class GeoMesaKuduInputFormat extends InputFormat[NullWritable, SimpleFeature] with LazyLogging {

  private val delegate = new KuduTableInputFormat()

  var sft: SimpleFeatureType = _
//  var table: HBaseFeatureIndex = _

  private def init(conf: Configuration): Unit = if (sft == null) {
//    sft = GeoMesaConfigurator.getSchema(conf)
//    table = HBaseFeatureIndex.index(GeoMesaConfigurator.getIndexIn(conf))
//    delegate.setConf(conf)
//    // see TableMapReduceUtil.java
//    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))
//    HBaseConnectionPool.configureSecurity(conf)
//    conf.set(TableInputFormat.INPUT_TABLE, GeoMesaConfigurator.getTable(conf))
  }

  override def getSplits(context: JobContext): java.util.List[InputSplit] = delegate.getSplits(context)

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[NullWritable, SimpleFeature] = {
//    init(context.getConfiguration)
//    val rr = delegate.createRecordReader(split, context)
//    val transformSchema = GeoMesaConfigurator.getTransformSchema(context.getConfiguration)
//    val schema = transformSchema.getOrElse(sft)
//    val q = GeoMesaConfigurator.getFilter(context.getConfiguration).map { f => ECQL.toFilter(f) }
//    // transforms are pushed down in HBase
//    new HBaseGeoMesaRecordReader(schema, table, rr, q, None)
//val ResultAdapter(cols, toFeatures) =
//KuduResultAdapter.resultsToFeatures(sft, schema, ecql, hints.getTransform, auths)
  }
}

object GeoMesaKuduInputFormat extends LazyLogging {

  import scala.collection.JavaConverters._
//
//  object Config {
//    val ColumnsKey = "geomesa.columns"
//  }

  def configure(conf: Configuration, params: Map[String, String], query: Query): Unit = {
    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KuduDataStore]
    require(ds != null, s"Could not create data store with provided parameters: ${params.mkString(", ")}")
    try { configure(conf, ds, query) } finally {
      ds.dispose()
    }
  }

  def configure(conf: Configuration, ds: KuduDataStore, query: Query): Unit = {
    val plans = ds.getQueryPlan(query)
    val plan = if (plans.isEmpty) {
      throw new IllegalArgumentException("Query resulted in nothing scanned")
    } else if (plans.lengthCompare(1) > 0) {
      // this query requires multiple scans, which we can't execute in a single scan
      // instead, fall back to a full table scan
      // TODO support multiple query plans?
      logger.warn("Desired query plan requires multiple scans - falling back to full table scan")
      val fallbackIndex = {
        val schema = ds.getSchema(query.getTypeName)
        KuduFeatureIndex.indices(schema, mode = IndexMode.Read).headOption.getOrElse {
          throw new IllegalStateException(s"Schema '${schema.getTypeName}' does not have any readable indices")
        }
      }

      val qps = ds.getQueryPlan(query, Some(fallbackIndex))
      if (qps.lengthCompare(1) > 0) {
        logger.error("The query being executed requires multiple scans, which is not currently " +
            "supported by GeoMesa. Your result set will be partially incomplete. " +
            s"Query: ${filterToString(query.getFilter)}")
      }
      qps.head
    } else {
      plans.head
    }

    configure(conf, ds.getSchema(query.getTypeName), plan)
  }

  def configure(conf: Configuration, master: String, sft: SimpleFeatureType, plan: KuduQueryPlan): Unit = {
    GeoMesaConfigurator.setSchema(conf, sft)
    plan.ecql.foreach(f => GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(f)))

    conf.set("kudu.mapreduce.master.address", master)
    conf.set("kudu.mapreduce.input.table", plan.table)
    conf.set("kudu.mapreduce.column.projection", plan.columns.mkString(","))

    plan.predicates ++ plan.ranges
    conf.set(KuduTableInputFormat.ENCODED_PREDICATES_KEY, base64EncodePredicates(predicates))

//    conf.setLong(KuduTableInputFormat.OPERATION_TIMEOUT_MS_KEY, operationTimeoutMs)
//    conf.setBoolean(KuduTableInputFormat.SCAN_CACHE_BLOCKS, cacheBlocks)
//    conf.setBoolean(KuduTableInputFormat.FAULT_TOLERANT_SCAN, isFaultTolerant)

    // TODO this operates on the job
    // KuduTableMapReduceUtil.addCredentialsToJob(masterAddresses, operationTimeoutMs)


  }

  class HBaseGeoMesaRecordReader(sft: SimpleFeatureType,
//                                 table: HBaseFeatureIndex,
//                                 reader: RecordReader[ImmutableBytesWritable, Result],
                                 filterOpt: Option[Filter],
                                 transformSchema: Option[SimpleFeatureType])
      extends RecordReader[Text, SimpleFeature] with LazyLogging {

    private var staged: SimpleFeature = _

    private val nextFeature =
      (filterOpt, transformSchema) match {
        case (Some(filter), Some(ts)) =>
          val indices = ts.getAttributeDescriptors.map { ad => sft.indexOf(ad.getLocalName) }
          val fn = table.toFeaturesWithFilterTransform(sft, filter, Array.empty[TransformProcess.Definition], indices.toArray, ts)
          nextFeatureFromOptional(fn)

        case (Some(filter), None) =>
          val fn = table.toFeaturesWithFilter(sft, filter)
          nextFeatureFromOptional(fn)

        case (None, Some(ts))         =>
          val indices = ts.getAttributeDescriptors.map { ad => sft.indexOf(ad.getLocalName) }
          val fn = table.toFeaturesWithTransform(sft, Array.empty[TransformProcess.Definition], indices.toArray, ts)
          nextFeatureFromDirect(fn)

        case (None, None)         =>
          val fn = table.toFeaturesDirect(sft)
          nextFeatureFromDirect(fn)
      }

    private val getId = table.getIdFromRow(sft)

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = reader.initialize(split, context)

    override def getProgress: Float = reader.getProgress

    override def nextKeyValue(): Boolean = nextKeyValueInternal()

    override def getCurrentValue: SimpleFeature = staged

    override def getCurrentKey = new Text(staged.getID)

    override def close(): Unit = reader.close()

    /**
      * Get the next key value from the underlying reader, incrementing the reader when required
      */
    private def nextKeyValueInternal(): Boolean = {
      nextFeature()
      if (staged != null) {
        val row = reader.getCurrentKey
        val offset = row.getOffset
        val length = row.getLength
        staged.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.get(), offset, length, staged))
        true
      } else {
        false
      }
    }

    private def nextFeatureFromOptional(toFeature: Result => Option[SimpleFeature]) = () => {
      staged = null
      while (staged == null && reader.nextKeyValue()) {
        try {
          toFeature(reader.getCurrentValue) match {
            case Some(feature) => staged = feature
            case None => staged = null
          }
        } catch {
          case NonFatal(e) => logger.error(s"Error reading row: ${reader.getCurrentValue}", e)
        }
      }
    }

    private def nextFeatureFromDirect(toFeature: Result => SimpleFeature) = () => {
      staged = null
      while (staged == null && reader.nextKeyValue()) {
        try {
          staged = toFeature(reader.getCurrentValue)
        } catch {
          case NonFatal(e) => logger.error(s"Error reading row: ${reader.getCurrentValue}", e)
        }
      }
    }
  }
}