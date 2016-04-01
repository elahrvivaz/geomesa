/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.io.IOException
import java.util.{List => jList}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client._
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.{FeatureTypes, NameImpl}
import org.joda.time.DateTimeUtils
import org.locationtech.geomesa.CURRENT_SCHEMA_VERSION
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore._
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data.stats.{GeoMesaMetadataStats, GeoMesaStats, HasGeoMesaStats, StatsRunner}
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.stats.{QueryStat, Stat, StatWriter}
import org.locationtech.geomesa.accumulo.util.{DistributedLocking, GeoMesaBatchWriterConfig, Releasable}
import org.locationtech.geomesa.accumulo.{AccumuloVersion, GeomesaSystemProperties}
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureSerializers}
import org.locationtech.geomesa.security.{AuditProvider, AuthorizationsProvider}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{FeatureSpec, NonGeomAttributeSpec}
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

/**
 * This class handles DataStores which are stored in Accumulo Tables. To be clear, one table may
 * contain multiple features addressed by their featureName.
 *
 * @param connector Accumulo connector
 * @param catalogTable Table name in Accumulo to store metadata about featureTypes. For pre-catalog
 *                     single-table stores this equates to the spatiotemporal table name
 * @param authProvider Provides the authorizations used to access data
 * @param auditProvider Provides access to the current user for auditing purposes
 * @param defaultVisibilities default visibilities applied to any data written
 * @param config configuration values
 */
class AccumuloDataStore(val connector: Connector,
                        val catalogTable: String,
                        val authProvider: AuthorizationsProvider,
                        val auditProvider: AuditProvider,
                        val defaultVisibilities: String,
                        val config: AccumuloDataStoreConfig)
    extends DataStore with AccumuloConnectorCreator with DistributedLocking
      with HasGeoMesaMetadata with HasGeoMesaStats with LazyLogging {

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  // having at least as many shards as tservers provides optimal parallelism in queries
  private val defaultMaxShard = connector.instanceOperations().getTabletServers.size()

  private val queryTimeoutMillis: Option[Long] = config.queryTimeout
      .orElse(GeomesaSystemProperties.QueryProperties.QUERY_TIMEOUT_MILLIS.option.map(_.toLong))

  private val defaultBWConfig = GeoMesaBatchWriterConfig().setMaxWriteThreads(config.writeThreads)

  private val tableOps = connector.tableOperations()

  override val metadata: GeoMesaMetadata = new AccumuloBackedMetadata(connector, catalogTable)
  override val stats: GeoMesaStats = new GeoMesaMetadataStats(this)

  private val statsRunner = new StatsRunner(this)

  // methods from org.geotools.data.DataStore

  /**
   * @see org.geotools.data.DataStore#getTypeNames()
   * @return existing simple feature type names
   */
  override def getTypeNames: Array[String] = metadata.getFeatureTypes

  /**
   * @see org.geotools.data.DataAccess#getNames()
   * @return existing simple feature type names
   */
  override def getNames: jList[Name] = getTypeNames.map(new NameImpl(_)).toList

  /**
   * Compute the GeoMesa SpatioTemporal Schema, create tables, and write metadata to catalog.
   * If the schema already exists, log a message and continue without error.
   * This method uses distributed locking to ensure a schema is only created once.
   *
   * @see org.geotools.data.DataAccess#createSchema(org.opengis.feature.type.FeatureType)
   * @param sft type to create
   */
  override def createSchema(sft: SimpleFeatureType): Unit = {
    if (getSchema(sft.getTypeName) == null) {
      val lock = acquireLock()
      try {
        // check a second time now that we have the lock
        if (getSchema(sft.getTypeName) == null) {
          // inspect, warn and set SF_PROPERTY_START_TIME if appropriate
          // do this before anything else so appropriate tables will be created
          TemporalIndexCheck.validateDtgField(sft)
          // set the schema version, required for table checks
          sft.setSchemaVersion(CURRENT_SCHEMA_VERSION)
          // get the requested index schema or build the default
          val spatioTemporalSchema = Option(sft.getStIndexSchema) match {
            case None         => buildDefaultSpatioTemporalSchema(sft.getTypeName)
            case Some(schema) => schema
          }
          checkSchemaRequirements(sft, spatioTemporalSchema)
          writeMetadata(sft, spatioTemporalSchema)

          // reload the SFT then copy over any additional keys that were in the original sft
          val reloadedSft = getSchema(sft.getTypeName)
          (sft.getUserData.keySet -- reloadedSft.getUserData.keySet)
              .foreach(k => reloadedSft.getUserData.put(k, sft.getUserData.get(k)))

          // create the tables in accumulo
          GeoMesaTable.getTables(reloadedSft).foreach { table =>
            val name = table.formatTableName(catalogTable, reloadedSft)
            AccumuloVersion.ensureTableExists(connector, name)
            table.configureTable(reloadedSft, name, tableOps)
          }
        }
      } finally {
        lock.release()
      }
    }
  }

  /**
   * @see org.geotools.data.DataStore#getSchema(java.lang.String)
   * @param typeName feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(typeName: String): SimpleFeatureType = getSchema(new NameImpl(typeName))

  /**
   * @see org.geotools.data.DataAccess#getSchema(org.opengis.feature.type.Name)
   * @param name feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(name: Name): SimpleFeatureType = {
    val typeName = name.getLocalPart
    metadata.readString(typeName, ATTRIBUTES_KEY).map { attributes =>
      val sft = SimpleFeatureTypes.createType(name.getURI, attributes)

      // IMPORTANT: set data that we want to pass around with the sft
      metadata.readString(typeName, DTGFIELD_KEY).foreach(sft.setDtgField)
      val version = metadata.readRequiredString(typeName, VERSION_KEY).toInt
      if (version > CURRENT_SCHEMA_VERSION) {
        logger.error(s"Trying to access schema ${sft.getTypeName} with version $version " +
            s"but client can only handle up to version $CURRENT_SCHEMA_VERSION.")
        throw new IllegalStateException(s"The schema ${sft.getTypeName} was written with a newer " +
            "version of GeoMesa than this client can handle. Please ensure that you are using the " +
            "same GeoMesa jar versions across your entire workflow. For more information, see " +
            "http://www.geomesa.org/documentation/user/installation_and_configuration.html#upgrading")
      } else {
        sft.setSchemaVersion(version)
      }
      sft.setStIndexSchema(metadata.readString(typeName, SCHEMA_KEY).orNull)
      // If no data is written, we default to 'false' in order to support old tables.
      if (metadata.readString(typeName, SHARED_TABLES_KEY).exists(_.toBoolean)) {
        sft.setTableSharing(true)
        // use schema id if available or fall back to old type name for backwards compatibility
        val prefix = metadata.readString(typeName, SCHEMA_ID_KEY).getOrElse(s"${sft.getTypeName}~")
        sft.setTableSharingPrefix(prefix)
      } else {
        sft.setTableSharing(false)
        sft.setTableSharingPrefix("")
      }
      metadata.readString(typeName, TABLES_ENABLED_KEY).foreach(sft.setEnabledTables)

      // back compatibility check for stat configuration
      if (metadata.read(typeName, STATS_GENERATION_KEY).isEmpty) {
        val lock = acquireLock()
        try {
          // configure the stats combining iterator on the table for this sft
          metadata.insert(typeName, STATS_GENERATION_KEY, GeoToolsDateFormat.print(Z3Table.EPOCH))
          GeoMesaMetadataStats.configureStatCombiner(tableOps, catalogTable, sft)
        } finally {
          lock.release()
        }
        // schedule an initial stat run for the existing data
        statsRunner.submit(sft)
      }
      sft
    }.orNull
  }

  /**
   * @see org.geotools.data.DataStore#updateSchema(java.lang.String, org.opengis.feature.simple.SimpleFeatureType)
   * @param typeName simple feature type name
   * @param sft new simple feature type
   */
  override def updateSchema(typeName: String, sft: SimpleFeatureType): Unit =
    updateSchema(new NameImpl(typeName), sft)

  /**
   * @see org.geotools.data.DataAccess#updateSchema(org.opengis.feature.type.Name, org.opengis.feature.type.FeatureType)
   * @param typeName simple feature type name
   * @param sft new simple feature type
   */
  override def updateSchema(typeName: Name, sft: SimpleFeatureType): Unit =
    throw new UnsupportedOperationException("AccumuloDataStore does not support updateSchema")

  /**
   * Deletes all features from the accumulo index tables and deletes metadata from the catalog.
   * If the feature type shares tables with another, this is fairly expensive,
   * proportional to the number of features. Otherwise, it is fairly cheap.
   *
   * @see org.geotools.data.DataStore#removeSchema(java.lang.String)
   * @param typeName simple feature type name
   */
  override def removeSchema(typeName: String) = {
    val lock = acquireLock()
    try {
      Option(getSchema(typeName)).foreach { sft =>
        if (sft.isTableSharing && getTypeNames.filter(_ != typeName).map(getSchema).exists(_.isTableSharing)) {
          deleteSharedTables(sft)
        } else {
          deleteStandAloneTables(sft)
        }
      }
      metadata.delete(typeName)
    } finally {
      lock.release()
    }
  }

  /**
   * @see org.geotools.data.DataAccess#removeSchema(org.opengis.feature.type.Name)
   * @param typeName simple feature type name
   */
  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  /**
   * @see org.geotools.data.DataStore#getFeatureSource(org.opengis.feature.type.Name)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: Name): AccumuloFeatureStore = {
    checkSchema(typeName.getLocalPart)
    if (config.caching) {
      new AccumuloFeatureStore(this, typeName) with CachingFeatureSource
    } else {
      new AccumuloFeatureStore(this, typeName)
    }
  }

  /**
   * @see org.geotools.data.DataStore#getFeatureSource(java.lang.String)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: String): AccumuloFeatureStore =
    getFeatureSource(new NameImpl(typeName))

  /**
   * @see org.geotools.data.DataStore#getFeatureReader(org.geotools.data.Query, org.geotools.data.Transaction)
   * @param query query to execute
   * @param transaction transaction to use (currently ignored)
   * @return feature reader
   */
  override def getFeatureReader(query: Query, transaction: Transaction): AccumuloFeatureReader = {
    val qp = getQueryPlanner(query.getTypeName)
    val stats = Some(this).collect { case w: StatWriter => w }.map((_, auditProvider))
    AccumuloFeatureReader(query, qp, queryTimeoutMillis, stats)
  }

  /**
   * Create a general purpose writer that is capable of updates and deletes.
   * Does <b>not</b> allow inserts. Will return all existing features.
   *
   * @see org.geotools.data.DataStore#getFeatureWriter(java.lang.String, org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriter(typeName: String, transaction: Transaction): AccumuloFeatureWriter =
    getFeatureWriter(typeName, Filter.INCLUDE, transaction)

  /**
   * Create a general purpose writer that is capable of updates and deletes.
   * Does <b>not</b> allow inserts.
   *
   * @see org.geotools.data.DataStore#getFeatureWriter(java.lang.String, org.opengis.filter.Filter,
   *        org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param filter cql filter to select features for update/delete
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): AccumuloFeatureWriter = {
    checkSchema(typeName)
    val sft = getSchema(typeName)
    val fe = SimpleFeatureSerializers(sft, getFeatureEncoding(sft))
    new ModifyAccumuloFeatureWriter(sft, fe, this, defaultVisibilities, filter)
  }

  /**
   * Creates a feature writer only for writing - does not allow updates or deletes.
   *
   * @see org.geotools.data.DataStore#getFeatureWriterAppend(java.lang.String, org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): AccumuloFeatureWriter = {
    checkSchema(typeName)
    val sft = getSchema(typeName)
    val fe = SimpleFeatureSerializers(sft, getFeatureEncoding(sft))
    new AppendAccumuloFeatureWriter(sft, fe, this, defaultVisibilities)
  }

  /**
   * @see org.geotools.data.DataAccess#getInfo()
   * @return service info
   */
  override def getInfo: ServiceInfo = {
    val info = new DefaultServiceInfo()
    info.setDescription("Features from AccumuloDataStore")
    info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
    info
  }

  /**
   * We always return null, which indicates that we are handling transactions ourselves.
   *
   * @see org.geotools.data.DataStore#getLockingManager()
   * @return locking manager - null
   */
  override def getLockingManager: LockingManager = null

  /**
   * Cleanup any open connections, etc. Equivalent to java.io.Closeable.close()
   *
   * @see org.geotools.data.DataAccess#dispose()
   */
  override def dispose(): Unit = statsRunner.close()

  // end methods from org.geotools.data.DataStore

  // methods from AccumuloConnectorCreator

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getScanner(java.lang.String)
   * @param table table to scan
   * @return scanner
   */
  override def getScanner(table: String): Scanner =
    connector.createScanner(table, authProvider.getAuthorizations)

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getBatchScanner(java.lang.String, int)
   * @param table table to scan
   * @param threads number of threads to use in scanning
   * @return batch scanner
   */
  override def getBatchScanner(table: String, threads: Int): BatchScanner =
    connector.createBatchScanner(table, authProvider.getAuthorizations, threads)

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getTableName(java.lang.String,
   *          org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable)
   * @param featureName feature type name
   * @param table table
   * @return accumulo table name
   */
  override def getTableName(featureName: String, table: GeoMesaTable): String = {
    val key = table match {
      case RecordTable         => RECORD_TABLE_KEY
      case Z3Table             => Z3_TABLE_KEY
      case AttributeTable      => ATTR_IDX_TABLE_KEY
      // noinspection ScalaDeprecation
      case AttributeTableV5    => ATTR_IDX_TABLE_KEY
      case SpatioTemporalTable => ST_IDX_TABLE_KEY
      case _ => throw new NotImplementedError("Unknown table")
    }
    metadata.readRequiredString(featureName, key)
  }

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getSuggestedThreads(java.lang.String,
   *          org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable)
   * @param featureName feature type name
   * @param table table
   * @return threads to use in scanning
   */
  override def getSuggestedThreads(featureName: String, table: GeoMesaTable): Int = {
    table match {
      case RecordTable         => config.recordThreads
      case Z3Table             => config.queryThreads
      case AttributeTable      => config.queryThreads
      // noinspection ScalaDeprecation
      case AttributeTableV5    => 1
      case SpatioTemporalTable => config.queryThreads
      case _ => throw new NotImplementedError("Unknown table")
    }
  }

  // end methods from AccumuloConnectorCreator

  /**
   * Method from StatWriter - allows us to mixin stat writing
   *
   * @see org.locationtech.geomesa.accumulo.stats.StatWriter.getStatTable
   */
  def getStatTable(stat: Stat): String = {
    stat match {
      case qs: QueryStat =>
        metadata.readString(stat.typeName, QUERIES_TABLE_KEY).getOrElse {
          // For backwards compatibility with existing tables that do not have queries table metadata
          val queriesTableValue = formatQueriesTableName(catalogTable)
          metadata.insert(stat.typeName, QUERIES_TABLE_KEY, queriesTableValue) // side effect
          queriesTableValue
        }
      case _ => throw new NotImplementedError()
    }
  }

  // other public methods

  /**
   * Optimized method to delete everything (all tables) associated with this datastore
   * (index tables and catalog table)
   * NB: We are *not* currently deleting the query table and/or query information.
   */
  def delete() = {
    val indexTables = getTypeNames.map(getSchema).flatMap(GeoMesaTable.getTableNames(_, this)).distinct
    // Delete index tables first then catalog table in case of error
    indexTables.filter(tableOps.exists).foreach(tableOps.delete)
    if (tableOps.exists(catalogTable)) {
      tableOps.delete(catalogTable)
    }
  }

  /**
   * Reads the serialization type (feature encoding) from the metadata.
   *
   * @param sft simple feature type
   * @return serialization type
   * @throws RuntimeException if the feature encoding is missing or invalid
   */
  @throws[RuntimeException]
  def getFeatureEncoding(sft: SimpleFeatureType): SerializationType = {
    metadata.readString(sft.getTypeName, "featureEncoding")
        .map(SerializationType.withName)
        .getOrElse(SerializationType.KRYO)
  }

  /**
   * Reads the index schema format out of the metadata
   *
   * @param typeName simple feature type name
   * @return index schema format string
   */
  def getIndexSchemaFmt(typeName: String): String =
    metadata.readString(typeName, SCHEMA_KEY).getOrElse(EMPTY_STRING)

  /**
   * Used to update the attributes that are marked as indexed - partial implementation of updateSchema.
   *
   * @param typeName feature type name
   * @param attributes attribute string, as is passed to SimpleFeatureTypes.createType
   */
  def updateIndexedAttributes(typeName: String, attributes: String): Unit = {
    val FeatureSpec(existing, _) = SimpleFeatureTypes.parse(metadata.readString(typeName, ATTRIBUTES_KEY).getOrElse(""))
    val FeatureSpec(updated, _)  = SimpleFeatureTypes.parse(attributes)
    // check that the only changes are to non-geometry index flags
    val ok = existing.length == updated.length &&
        existing.zip(updated).forall { case (e, u) => e == u ||
            (e.isInstanceOf[NonGeomAttributeSpec] &&
                u.isInstanceOf[NonGeomAttributeSpec] &&
                e.clazz == u.clazz &&
                e.name == u.name) }
    if (!ok) {
      throw new IllegalArgumentException("Attribute spec is not consistent with existing spec")
    }

    metadata.insert(typeName, ATTRIBUTES_KEY, attributes)

    // reconfigure the splits on the attribute table
    val sft = getSchema(typeName)
    val table = getTableName(typeName, AttributeTable)
    AttributeTable.configureTable(sft, table, tableOps)
  }

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against accumulo.
   *
   * @param query query to execute
   * @param strategy hint on the index to use to satisfy the query
   * @return query plans
   */
  def getQueryPlan(query: Query,
                   strategy: Option[StrategyType] = None,
                   explainer: ExplainerOutputType = ExplainNull): Seq[QueryPlan] = {
    require(query.getTypeName != null, "Type name is required in the query")
    getQueryPlanner(query.getTypeName).planQuery(query, None, explainer)
  }

  /**
   * Gets a query planner. Also has side-effect of setting transforms in the query.
   */
  protected[data] def getQueryPlanner(featureName: String): QueryPlanner = {
    checkSchema(featureName)
    val sft = getSchema(featureName)
    val indexSchemaFmt = getIndexSchemaFmt(featureName)
    val featureEncoding = getFeatureEncoding(sft)
    new QueryPlanner(sft, featureEncoding, indexSchemaFmt, this, stats)
  }

  // end public methods

  // equivalent to: s"%~#s%$maxShard#r%${name}#cstr%0,3#gh%yyyyMMddHH#d::%~#s%3,2#gh::%~#s%#id"
  private def buildDefaultSpatioTemporalSchema(name: String, maxShard: Int = defaultMaxShard): String =
    new IndexSchemaBuilder("~")
        .randomNumber(maxShard)
        .indexOrDataFlag()
        .constant(name)
        .geoHash(0, 3)
        .date("yyyyMMddHH")
        .nextPart()
        .geoHash(3, 2)
        .nextPart()
        .id()
        .build()

  /**
   * Computes and writes the metadata for this feature type
   */
  private def writeMetadata(sft: SimpleFeatureType, spatioTemporalSchemaValue: String) {

    // compute the metadata values
    val attributesValue             = SimpleFeatureTypes.encodeType(sft)
    val dtgValue: Option[String]    = sft.getDtgField // this will have already been checked and set
    val z3TableValue                = Z3Table.formatTableName(catalogTable, sft)
    val spatioTemporalIdxTableValue = SpatioTemporalTable.formatTableName(catalogTable, sft)
    val attrIdxTableValue           = AttributeTable.formatTableName(catalogTable, sft)
    val recordTableValue            = RecordTable.formatTableName(catalogTable, sft)
    val queriesTableValue           = formatQueriesTableName(catalogTable)
    val tableSharingValue           = sft.isTableSharing.toString
    val enabledTablesValue          = sft.getEnabledTables.toString
    val dataStoreVersion            = CURRENT_SCHEMA_VERSION.toString

    // store each metadata in the associated key
    val attributeMap =
      Map(
        ATTRIBUTES_KEY        -> attributesValue,
        SCHEMA_KEY            -> spatioTemporalSchemaValue,
        Z3_TABLE_KEY          -> z3TableValue,
        ST_IDX_TABLE_KEY      -> spatioTemporalIdxTableValue,
        ATTR_IDX_TABLE_KEY    -> attrIdxTableValue,
        RECORD_TABLE_KEY      -> recordTableValue,
        QUERIES_TABLE_KEY     -> queriesTableValue,
        SHARED_TABLES_KEY     -> tableSharingValue,
        TABLES_ENABLED_KEY    -> enabledTablesValue,
        VERSION_KEY           -> dataStoreVersion
      ) ++ (if (dtgValue.isDefined) Map(DTGFIELD_KEY -> dtgValue.get) else Map.empty)

    val featureName = sft.getTypeName
    metadata.insert(featureName, attributeMap)

    // write a schema ID out - ensure that it is unique in this catalog
    // IMPORTANT: this method needs to stay inside a zookeeper distributed locking block
    var schemaId = 1
    val existingSchemaIds =
      getTypeNames.flatMap(metadata.readString(_, SCHEMA_ID_KEY, cache = false).map(_.getBytes("UTF-8").head.toInt))
    while (existingSchemaIds.contains(schemaId)) { schemaId += 1 }
    // We use a single byte for the row prefix to save space - if we exceed the single byte limit then
    // our ranges would start to overlap and we'd get errors
    require(schemaId <= Byte.MaxValue,
      s"No more than ${Byte.MaxValue} schemas may share a single catalog table")
    metadata.insert(featureName, SCHEMA_ID_KEY, new String(Array(schemaId.asInstanceOf[Byte]), "UTF-8"))

    // configure the stats combining iterator on the table for this sft
    metadata.insert(featureName, STATS_GENERATION_KEY, GeoToolsDateFormat.print(DateTimeUtils.currentTimeMillis()))
    GeoMesaMetadataStats.configureStatCombiner(tableOps, catalogTable, sft)
  }

  // This function enforces the shared ST schema requirements.
  // For a shared ST table, the IndexSchema must start with a partition number and a constant string.
  // TODO: This function should check if the constant is equal to the featureType.getTypeName
  private def checkSchemaRequirements(featureType: SimpleFeatureType, schema: String) {
    if (featureType.isTableSharing) {
      val (rowf, _,_) = IndexSchema.parse(IndexSchema.formatter, schema).get
      rowf.lf match {
        case Seq(pf: PartitionTextFormatter, i: IndexOrDataTextFormatter, const: ConstantTextFormatter, r@_*) =>
        case _ => throw new RuntimeException(s"Failed to validate the schema requirements for " +
          s"the feature ${featureType.getTypeName} for catalog table : $catalogTable.  " +
          s"We require that features sharing a table have schema starting with a partition and a constant.")
      }
    }
  }

  private def deleteSharedTables(sft: SimpleFeatureType) = {
    val auths = authProvider.getAuthorizations
    GeoMesaTable.getTables(sft).par.foreach { table =>
      val name = getTableName(sft.getTypeName, table)
      if (tableOps.exists(name)) {
        if (table == Z3Table) {
          tableOps.delete(name)
        } else {
          val deleter = connector.createBatchDeleter(name, auths, config.queryThreads, defaultBWConfig)
          table.deleteFeaturesForType(sft, deleter)
          deleter.close()
        }
      }
    }
  }

  // NB: We are *not* currently deleting the query table and/or query information.
  private def deleteStandAloneTables(sft: SimpleFeatureType) =
    GeoMesaTable.getTableNames(sft, this).filter(tableOps.exists).foreach(tableOps.delete)

  /**
   * Verifies that the schema exists
   *
   * @param typeName simple feature type
   * @throws IOException if schema does not exist
   */
  @throws[IOException]
  private def checkSchema(typeName: String): Unit = {
    def getAttribute(cache: Boolean): Option[String] = metadata.readString(typeName, ATTRIBUTES_KEY, cache)
    if (getAttribute(true).isEmpty && getAttribute(false).isEmpty) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
  }

  private def acquireLock(): Releasable = {
    val path = s"/org.locationtech.geomesa/accumulo/ds/$catalogTable"
    acquireLock(path, 120000).getOrElse(throw new RuntimeException(s"Could not acquire distributed lock at '$path'"))
  }
}

object AccumuloDataStore {

  /**
   * Format queries table name for Accumulo...table name is stored in metadata for other usage
   * and provide compatibility moving forward if table names change
   *
   * @param catalogTable table
   * @return formatted name
   */
  def formatQueriesTableName(catalogTable: String): String =
    GeoMesaTable.concatenateNameParts(catalogTable, "queries")
}

// record scans are single-row ranges - increasing the threads too much actually causes performance to decrease
case class AccumuloDataStoreConfig(queryTimeout: Option[Long],
                                   queryThreads: Int,
                                   recordThreads: Int,
                                   writeThreads: Int,
                                   caching: Boolean)
