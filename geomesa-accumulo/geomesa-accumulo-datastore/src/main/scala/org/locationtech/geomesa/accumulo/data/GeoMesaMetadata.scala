/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{Connector, IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Mutation, Range, Value}
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.AccumuloBackedMetadata._
import org.locationtech.geomesa.accumulo.data.stats.StatsCombiner
import org.locationtech.geomesa.accumulo.util.{EmptyScanner, GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConversions._

/**
 * GeoMesa Metadata/Catalog abstraction using key/value String pairs storing
 * them on a per-featurename basis
 */
trait GeoMesaMetadata {

  /**
   * Returns existing simple feature types
   *
   * @return simple feature type names
   */
  def getFeatureTypes: Array[String]

  /**
   * Insert a value - any existing value under the given key will be overwritten
   *
   * @param featureName simple feature type name
   * @param key key
   * @param value value
   */
  def insert(featureName: String, key: String, value: String)

  /**
   * Insert multiple values at once - may be more efficient than single inserts
   *
   * @param featureName simple feature type name
   * @param kvPairs key/values
   */
  def insert(featureName: String, kvPairs: Map[String, String]): Unit

  /**
   * Reads a value
   *
   * @param featureName simple feature type name
   * @param key key
   * @param cache may return a cached value if true, otherwise may use a slower lookup
   * @return value, if present
   */
  def read(featureName: String, key: String, cache: Boolean = true): Option[String]

  /**
   * Reads a value. Throws an exception if value is missing
   *
   * @param featureName simple feature type name
   * @param key key
   * @return value
   */
  def readRequired(featureName: String, key: String): String

  /**
   * Deletes all values associated with a given feature type
   *
   * @param featureName simple feature type name
   */
  def delete(featureName: String)
}

object GeoMesaMetadata {
  val ATTRIBUTES_KEY            = "attributes"
  val SCHEMA_KEY                = "schema"
  val DTGFIELD_KEY              = "dtgfield"
  val ST_IDX_TABLE_KEY          = "tables.idx.st.name"
  val ATTR_IDX_TABLE_KEY        = "tables.idx.attr.name"
  val RECORD_TABLE_KEY          = "tables.record.name"
  val Z3_TABLE_KEY              = "tables.z3.name"
  val QUERIES_TABLE_KEY         = "tables.queries.name"
  val SHARED_TABLES_KEY         = "tables.sharing"
  val TABLES_ENABLED_KEY        = SimpleFeatureTypes.ENABLED_INDEXES
  val SCHEMA_ID_KEY             = "id"
  val VERSION_KEY               = "version"

  val BOUNDS_PREFIX             = "stats-b"
  val DISTRIBUTION_PREFIX       = "stats-d"
  val SPATIAL_BOUNDS_KEY        = s"$BOUNDS_PREFIX-spatial"
  val TEMPORAL_BOUNDS_KEY       = s"$BOUNDS_PREFIX-temporal"
  val SPATIAL_DISTRIBUTION_KEY  = s"$DISTRIBUTION_PREFIX-spatial"
  val TEMPORAL_DISTRIBUTION_KEY = s"$DISTRIBUTION_PREFIX-temporal"
  val STATS_GENERATION_KEY      = "stats-date"
  val STATS_INTERVAL_KEY        = "stats-interval"
}

trait HasGeoMesaMetadata {
  def metadata: GeoMesaMetadata
}

class AccumuloBackedMetadata(connector: Connector, catalogTable: String)
    extends GeoMesaMetadata with LazyLogging {

  import GeoMesaMetadata._

  // warning: only access this map in a synchronized fashion
  /*private */val metaDataCache = scala.collection.mutable.HashMap.empty[(String, String), Option[String]]

  private val metadataBWConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(1)

  // warning: only access in a synchronized fashion
  private var tableExists = false

  /**
   * Scans metadata rows and pulls out the different feature types in the table
   *
   * @return
   */
  override def getFeatureTypes: Array[String] = {
    val scanner = createScanner
    scanner.setRange(new Range(METADATA_TAG, METADATA_TAG_END))
    // restrict to just schema cf so we only get 1 hit per feature
    scanner.fetchColumnFamily(new Text(SCHEMA_KEY))
    try {
      scanner.map(kv => getFeatureNameFromMetadataRowKey(kv.getKey.getRow.toString)).toArray
    } finally {
      scanner.close()
    }
  }

  override def read(featureName: String, key: String, cache: Boolean): Option[String] = {
    if (cache) {
      metaDataCache.synchronized(metaDataCache.getOrElseUpdate((featureName, key), scanEntry(featureName, key)))
    } else {
      val res = scanEntry(featureName, key)
      metaDataCache.synchronized(metaDataCache.put((featureName, key), res))
      res
    }
  }

  override def readRequired(featureName: String, key: String): String =
    read(featureName, key)
      .getOrElse(throw new RuntimeException(s"Unable to find required metadata property for key $key"))

  override def insert(featureName: String, key: String, value: String): Unit =
    insert(featureName, Map(key -> value))

  override def insert(featureName: String, kvPairs: Map[String, String]): Unit = {
    checkTable(create = true)
    val delete = new Mutation(getMetadataRowKey(featureName))
    val insert = new Mutation(getMetadataRowKey(featureName))
    kvPairs.foreach { case (k,v) =>
      insert.put(new Text(k), EMPTY_COLQ, new Value(v.getBytes))
      // if it's a bounds query, we use a combiner to merge them
      // otherwise, explicitly delete any existing value
      if (!k.startsWith(BOUNDS_PREFIX)) {
        delete.putDelete(new Text(k), EMPTY_COLQ)
      }
    }
    val writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
    if (delete.size() > 0) {
      writer.addMutation(delete)
    }
    writer.addMutation(insert)
    writer.flush()
    writer.close()
    // also pre-fetch into the cache
    val toCache = kvPairs.map(kv => (featureName, kv._1) -> Option(kv._2).filterNot(_.isEmpty))
    metaDataCache.synchronized(metaDataCache.putAll(toCache))
  }

  /**
   * Handles deleting metadata from the catalog by using the Range obtained from the METADATA_TAG and featureName
   * and setting that as the Range to be handled and deleted by Accumulo's BatchDeleter
   *
   * @param featureName the name of the table to query and delete from
   */
  override def delete(featureName: String): Unit = {
    if (checkTable(create = false)) {
      val range = new Range(getMetadataRowKey(featureName))
      val deleter = connector.createBatchDeleter(catalogTable, AccumuloVersion.getEmptyAuths, 1, metadataBWConfig)
      deleter.setRanges(List(range))
      deleter.delete()
      deleter.close()
    } else {
      logger.warn(s"Trying to delete type '$featureName' from '$catalogTable' but table does not exist")
    }
    metaDataCache.synchronized {
      metaDataCache.keys.filter { case (fn, _) => fn == featureName}.foreach(metaDataCache.remove)
    }
  }

  /**
   * Reads a single key/value from the underlying table
   *
   * @param featureName simple feature type name
   * @param key key
   * @return value, if it exists
   */
  private def scanEntry(featureName: String, key: String): Option[String] = {
    val scanner = createScanner
    scanner.setRange(new Range(getMetadataRowKey(featureName)))
    scanner.fetchColumn(new Text(key), EMPTY_COLQ)
    SelfClosingIterator(scanner).map(_.getValue.toString).toList.headOption
  }

  /**
   * Create an Accumulo Scanner to the Catalog table to query Metadata for this store
   */
  private def createScanner: Scanner =
    if (checkTable(create = false)) {
      connector.createScanner(catalogTable, AccumuloVersion.getEmptyAuths)
    } else {
      EmptyScanner
    }

  /**
   * Verifies that the metadata table exists. Ensures that iterators are configured correctly
   * for statistics. Creates table if requested.
   *
   * @param create create the table if it doesn't exist
   * @return true if table exists after this method runs
   */
  private def checkTable(create: Boolean): Boolean = synchronized {
    if (tableExists) {
      true
    } else {
      val ops = connector.tableOperations()
      lazy val iterator = new IteratorSetting(10, classOf[StatsCombiner],
        Map("columns"-> Seq(SPATIAL_BOUNDS_KEY, TEMPORAL_BOUNDS_KEY).map(_ + ":").mkString(",")))
      tableExists = ops.exists(catalogTable)
      if (tableExists) {
        // verify that the correct iterators are configured - for back compatibility
        val iterators = ops.listIterators(catalogTable)
        Option(iterators.get(classOf[VersioningIterator].getSimpleName)).foreach { scopes =>
          ops.removeIterator(catalogTable, classOf[VersioningIterator].getSimpleName, scopes)
        }
        if (iterators.get(classOf[StatsCombiner].getSimpleName) == null) {
          ops.attachIterator(catalogTable, iterator)
        }
      } else if (create) {
        AccumuloVersion.ensureTableExists(connector, catalogTable, versioning = false)
        ops.attachIterator(catalogTable, iterator)
        tableExists = true
      }
      tableExists
    }
  }
}

object AccumuloBackedMetadata {

  private val METADATA_TAG     = "~METADATA"
  private val METADATA_TAG_END = s"$METADATA_TAG~~"

  private val MetadataRowKeyRegex = (METADATA_TAG + """_(.*)""").r

  /**
   * Reads the feature name from a given metadata row key
   *
   * @param rowKey row from accumulo
   * @return simple feature type name
   */
  private def getFeatureNameFromMetadataRowKey(rowKey: String): String = {
    val MetadataRowKeyRegex(featureName) = rowKey
    featureName
  }

  /**
   * Creates the row id for a metadata entry
   *
   * @param featureName simple feature type name
   * @return
   */
  private def getMetadataRowKey(featureName: String) = new Text(METADATA_TAG + "_" + featureName)
}
