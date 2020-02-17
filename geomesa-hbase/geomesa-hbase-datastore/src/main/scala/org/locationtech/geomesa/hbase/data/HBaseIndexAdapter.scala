/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import java.util.{Collections, Locale}

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, KeyOnlyFilter, MultiRowRangeFilter, Filter => HFilter}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.apache.hadoop.hbase._
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.HBaseSystemProperties.{CoprocessorPath, TableAvailabilityTimeout}
import org.locationtech.geomesa.hbase.coprocessor.aggregators.{HBaseArrowAggregator, HBaseBinAggregator, HBaseDensityAggregator, HBaseStatsAggregator}
import org.locationtech.geomesa.hbase.coprocessor.{AllCoprocessors, CoprocessorConfig, GeoMesaCoprocessor}
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.{CoprocessorPlan, EmptyPlan, ScanPlan}
import org.locationtech.geomesa.hbase.filters.{CqlTransformFilter, Z2HBaseFilter, Z3HBaseFilter}
import org.locationtech.geomesa.hbase.utils.HBaseVersions
import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api.{WritableFeature, _}
import org.locationtech.geomesa.index.filters.{Z2Filter, Z3Filter}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexValues}
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexValues}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.LocalQueryRunner.ArrowDictionaryHook
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FlushWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.NonFatal

class HBaseIndexAdapter(ds: HBaseDataStore) extends IndexAdapter[HBaseDataStore] with StrictLogging {

  import HBaseIndexAdapter._

  import scala.collection.JavaConverters._

  override def createTable(
      index: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      splits: => Seq[Array[Byte]]): Unit = {
    val table = index.configureTableName(partition, tableNameLimit) // write table name to metadata
    val name = TableName.valueOf(table)

    WithClose(ds.connection.getAdmin) { admin =>
      if (!admin.tableExists(name)) {
        logger.debug(s"Creating table $name")

        val conf = admin.getConfiguration
        val compression = Option(index.sft.getUserData.get(Configs.COMPRESSION_ENABLED)).collect {
          case e: String if e.toBoolean =>
            // note: all compression types in HBase are case-sensitive and lower-cased
            val compressionType = index.sft.getUserData.get(Configs.COMPRESSION_TYPE) match {
              case null => "gz"
              case t: String => t.toLowerCase(Locale.US)
            }
            logger.debug(s"Setting compression '$compressionType' on table $name for feature ${index.sft.getTypeName}")
            Compression.getCompressionAlgorithmByName(compressionType)
        }

        val descriptor = new HTableDescriptor(name)

        groups.apply(index.sft).foreach { case (k, _) =>
          val column = new HColumnDescriptor(k)
          column.setBloomFilterType(BloomType.NONE)
          compression.foreach(column.setCompressionType)
          if (index.name != IdIndex.name) {
            column.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
          }
          HBaseVersions.addFamily(descriptor, column)
        }

        if (ds.config.remoteFilter) {
          lazy val coprocessorUrl = ds.config.coprocessorUrl.orElse(CoprocessorPath.option.map(new Path(_))).orElse {
            try {
              // the jar should be under hbase.dynamic.jars.dir to enable filters, so look there
              val dir = new Path(conf.get("hbase.dynamic.jars.dir"))
              WithClose(dir.getFileSystem(conf)) { fs =>
                if (!fs.isDirectory(dir)) { None } else {
                  fs.listStatus(dir).collectFirst {
                    case s if distributedJarNamePattern.matcher(s.getPath.getName).matches() => s.getPath
                  }
                }
              }
            } catch {
              case NonFatal(e) => logger.warn("Error checking dynamic jar path:", e); None
            }
          }

          def addCoprocessor(clazz: Class[_ <: Coprocessor], desc: HTableDescriptor): Unit = {
            val name = clazz.getCanonicalName
            if (!desc.getCoprocessors.contains(name)) {
              logger.debug(s"Using coprocessor path ${coprocessorUrl.orNull}")
              // TODO: Warn if the path given is different from paths registered in other coprocessors
              // if so, other tables would need updating
              HBaseVersions.addCoprocessor(desc, name, coprocessorUrl)
            }
          }

          // if the coprocessors are installed site-wide don't register them in the table descriptor
          val installed = Option(conf.get(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY))
          val names = installed.map(_.split(":").toSet).getOrElse(Set.empty[String])
          AllCoprocessors.foreach(c => if (!names.contains(c.getCanonicalName)) { addCoprocessor(c, descriptor) })
        }

        try { admin.createTableAsync(descriptor, splits.toArray) } catch {
          case _: org.apache.hadoop.hbase.TableExistsException => // ignore, another thread created it for us
        }
      }

      // wait for the table to come online
      if (!admin.isTableAvailable(name)) {
        val timeout = TableAvailabilityTimeout.toDuration.filter(_.isFinite())
        logger.debug(s"Waiting for table '$table' to become available with " +
            s"${timeout.map(t => s"a timeout of $t").getOrElse("no timeout")}")
        val stop = timeout.map(t => System.currentTimeMillis() + t.toMillis)
        while (!admin.isTableAvailable(name) && stop.forall(_ > System.currentTimeMillis())) {
          Thread.sleep(1000)
        }
      }
    }
  }

  override def deleteTables(tables: Seq[String]): Unit = {
    WithClose(ds.connection.getAdmin) { admin =>
      tables.par.foreach { name =>
        val table = TableName.valueOf(name)
        if (admin.tableExists(table)) {
          admin.disableTableAsync(table)
          val timeout = TableAvailabilityTimeout.toDuration.filter(_.isFinite())
          logger.debug(s"Waiting for table '$table' to be disabled with " +
              s"${timeout.map(t => s"a timeout of $t").getOrElse("no timeout")}")
          val stop = timeout.map(t => System.currentTimeMillis() + t.toMillis)
          while (!admin.isTableDisabled(table) && stop.forall(_ > System.currentTimeMillis())) {
            Thread.sleep(1000)
          }
          // no async operation, but timeout can be controlled through hbase-site.xml "hbase.client.sync.wait.timeout.msec"
          admin.deleteTable(table)
        }
      }
    }
  }

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    tables.par.foreach { name =>
      val tableName = TableName.valueOf(name)
      WithClose(ds.connection.getTable(tableName)) { table =>
        val scan = new Scan().setFilter(new KeyOnlyFilter)
        prefix.foreach(scan.setRowPrefixFilter)
        ds.applySecurity(scan)
        val mutateParams = new BufferedMutatorParams(tableName)
        WithClose(table.getScanner(scan), ds.connection.getBufferedMutator(mutateParams)) { case (scanner, mutator) =>
          scanner.iterator.asScala.grouped(10000).foreach { result =>
            // TODO GEOMESA-2546 set delete visibilities
            val deletes = result.map(r => new Delete(r.getRow))
            mutator.mutate(deletes.asJava)
          }
        }
      }
    }
  }

  override def createQueryPlan(strategy: QueryStrategy): HBaseQueryPlan = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val QueryStrategy(filter, byteRanges, _, _, ecql, hints, _) = strategy
    val index = filter.index

    // index api defines empty start/end for open-ended range
    // index api defines start row inclusive, end row exclusive
    // both these conventions match the conventions for hbase scan objects
    val ranges = byteRanges.map {
      case BoundedByteRange(start, stop) => new RowRange(start, true, stop, false)
      case SingleRowByteRange(row)       => new RowRange(row, true, ByteArrays.rowFollowingRow(row), false)
    }
    val small = byteRanges.headOption.exists(_.isInstanceOf[SingleRowByteRange])

    val tables = index.getTablesForQuery(filter.filter).map(TableName.valueOf)
    val (colFamily, schema) = groups.group(index.sft, hints.getTransformDefinition, ecql)

    val transform: Option[(String, SimpleFeatureType)] = hints.getTransform

    if (!ds.config.remoteFilter) {
      // everything is done client side
      val resultsToFeatures: CloseableIterator[Result] => CloseableIterator[SimpleFeature] = rows => {
        val arrowHook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
        val features = filter.filter match {
          case None    => HBaseIndexAdapter.resultsToFeatures(index, schema)(rows)
          case Some(f) => HBaseIndexAdapter.resultsToFeatures(index, schema)(rows).filter(f.evaluate)
        }
        LocalQueryRunner.transform(schema, features, transform, hints, arrowHook)
      }
      if (tables.isEmpty || ranges.isEmpty) { EmptyPlan(strategy.filter, resultsToFeatures) } else {
        val scans = configureScans(tables, ranges, small, colFamily, Seq.empty, coprocessor = false)
        ScanPlan(filter, tables, ranges, scans, resultsToFeatures)
      }
    } else {
      lazy val returnSchema = transform.map(_._2).getOrElse(schema)
      lazy val timeout = strategy.index.ds.config.queryTimeout.map(GeoMesaCoprocessor.timeout)

      val coprocessorConfig = if (hints.isDensityQuery) {
        val options = HBaseDensityAggregator.configure(schema, index, ecql, hints)
        Some(CoprocessorConfig(options ++ timeout, HBaseDensityAggregator.bytesToFeatures))
      } else if (hints.isArrowQuery) {
        val (options, reduce) = HBaseArrowAggregator.configure(schema, index, ds.stats, filter.filter, ecql, hints)
        Some(CoprocessorConfig(options ++ timeout, HBaseArrowAggregator.bytesToFeatures, reduce))
      } else if (hints.isStatsQuery) {
        val options = HBaseStatsAggregator.configure(schema, index, ecql, hints)
        val reduce = StatsScan.reduceFeatures(returnSchema, hints) _
        Some(CoprocessorConfig(options ++ timeout, HBaseStatsAggregator.bytesToFeatures, reduce))
      } else if (hints.isBinQuery) {
        val options = HBaseBinAggregator.configure(schema, index, ecql, hints)
        Some(CoprocessorConfig(options ++ timeout, HBaseBinAggregator.bytesToFeatures))
      } else {
        None
      }

      val filters = {
        // if there is a coprocessorConfig it handles filter/transform
        val cqlFilter = if (coprocessorConfig.isDefined || (ecql.isEmpty && transform.isEmpty)) { Seq.empty } else {
          Seq((CqlTransformFilter.Priority, CqlTransformFilter(schema, strategy.index, ecql, transform)))
        }

        // TODO pull this out to be SPI loaded so that new indices can be added seamlessly
        val indexFilter = strategy.index match {
          case _: Z3Index =>
            strategy.values.toSeq.map { case v: Z3IndexValues =>
              (Z3HBaseFilter.Priority, Z3HBaseFilter(Z3Filter(v), index.keySpace.sharding.length))
            }

          case _: Z2Index =>
            strategy.values.toSeq.map { case v: Z2IndexValues =>
              (Z2HBaseFilter.Priority, Z2HBaseFilter(Z2Filter(v), index.keySpace.sharding.length))
            }

          // TODO GEOMESA-1807 deal with non-points in a pushdown XZ filter

          case _ => Seq.empty
        }

        (cqlFilter ++ indexFilter).sortBy(_._1).map(_._2)
      }

      coprocessorConfig match {
        case None =>
          val resultsToFeatures = HBaseIndexAdapter.resultsToFeatures(index, returnSchema)
          if (tables.isEmpty || ranges.isEmpty) { EmptyPlan(strategy.filter, resultsToFeatures) } else {
            val scans = configureScans(tables, ranges, small, colFamily, filters, coprocessor = false)
            ScanPlan(filter, tables, ranges, scans, resultsToFeatures)
          }

        case Some(c) =>
          if (tables.isEmpty || ranges.isEmpty) {
            EmptyPlan(strategy.filter, _ => c.reduce(CloseableIterator.empty))
          } else {
            val scans = configureScans(tables, ranges, small, colFamily, filters, coprocessor = true)
            CoprocessorPlan(filter, tables, ranges, scans, c)
          }
      }
    }
  }

  override def createWriter(sft: SimpleFeatureType,
                            indices: Seq[GeoMesaFeatureIndex[_, _]],
                            partition: Option[String]): HBaseIndexWriter =
    new HBaseIndexWriter(ds, indices, WritableFeature.wrapper(sft, groups), partition)

  /**
   * Configure the hbase scan
   *
   * @param tables tables being scanned, used for region location information
   * @param ranges ranges to scan, non-empty. needs to be mutable as we will sort it in place
   * @param small whether 'small' ranges (i.e. gets)
   * @param colFamily col family to scan
   * @param filters scan filters
   * @param coprocessor is this a coprocessor scan or not
   * @return
   */
  protected def configureScans(
      tables: Seq[TableName],
      ranges: Seq[RowRange],
      small: Boolean,
      colFamily: Array[Byte],
      filters: Seq[HFilter],
      coprocessor: Boolean): Seq[Scan] = {

    val cacheBlocks = HBaseSystemProperties.ScannerBlockCaching.toBoolean.get // has a default value so .get is safe
    val cacheSize = HBaseSystemProperties.ScannerCaching.toInt

    logger.debug(s"HBase client scanner: block caching: $cacheBlocks, caching: $cacheSize")

    if (small && !coprocessor) {
      val filter = filters match {
        case Nil    => null
        case Seq(f) => f
        case f      => new FilterList(f: _*)
      }
      ranges.map { r =>
        val scan = new Scan(r.getStartRow, r.getStopRow)
        scan.addFamily(colFamily).setCacheBlocks(cacheBlocks).setSmall(true)
        scan.setFilter(filter)
        cacheSize.foreach(scan.setCaching)
        ds.applySecurity(scan)
        scan
      }
    } else {

      // split and group ranges by region server
      val rangesPerRegionServer = scala.collection.mutable.Map.empty[ServerName, ListBuffer[RowRange]]

      WithClose(ds.connection.getRegionLocator(tables.head)) { locator =>
        val iter = ranges.iterator
        while (iter.hasNext) {
          var range = iter.next
          try {
            var region = locator.getRegionLocation(range.getStartRow)
            while (region.getRegionInfo.getEndKey.length > 0 &&
                ByteArrays.ByteOrdering.compare(region.getRegionInfo.getEndKey, range.getStopRow) < 0) {
              // split the range based on the current region
              rangesPerRegionServer.getOrElseUpdate(region.getServerName, ListBuffer.empty) +=
                  new RowRange(range.getStartRow, true, region.getRegionInfo.getEndKey, false)
              range = new RowRange(region.getRegionInfo.getEndKey, true, range.getStopRow, false)
              region = locator.getRegionLocation(range.getStartRow)
            }
            rangesPerRegionServer.getOrElseUpdate(region.getServerName, ListBuffer.empty) += range
          } catch {
            case NonFatal(e) =>
              logger.warn(s"Error checking range location for '$range''", e)
              rangesPerRegionServer.getOrElseUpdate(null, ListBuffer.empty) += range
          }
        }
      }

      val groupedScans = Seq.newBuilder[Scan]

      def addGroup(group: java.util.List[RowRange]): Unit = {
        val s = new Scan(group.get(0).getStartRow, group.get(group.size() - 1).getStopRow)
        val mrrf = if (group.size() < 2) { Seq.empty } else {
          // TODO GEOMESA-1806
          // currently, the MultiRowRangeFilter constructor will call sortAndMerge a second time
          // this is unnecessary as we have already sorted and merged
          Seq(new MultiRowRangeFilter(group))
        }
        // note: mrrf first priority
        val combinedFilters = mrrf ++ filters
        // note: coprocessors always expect a filter list
        val filter = if (coprocessor) { new FilterList(combinedFilters: _*) } else {
          combinedFilters match {
            case Nil    => null
            case Seq(f) => f
            case f      => new FilterList(f: _*)
          }
        }
        s.setFilter(filter)
        s.addFamily(colFamily).setCacheBlocks(cacheBlocks)
        cacheSize.foreach(s.setCaching)

        // apply visibilities
        ds.applySecurity(s)

        groupedScans += s
      }

      // for coprocessors, we want 1 scan per region server
      val maxRangesPerGroup = if (coprocessor) { Int.MaxValue } else {
        val totalRanges = rangesPerRegionServer.values.map(_.length).sum
        math.min(ds.config.maxRangesPerExtendedScan,
          math.max(1, math.ceil(totalRanges.toDouble / ds.config.queryThreads).toInt))
      }

      rangesPerRegionServer.foreach { case (_, ranges) =>
        val list = new java.util.ArrayList[RowRange](ranges.asJava)
        // our ranges are non-overlapping, so just sort them but don't bother merging them
        Collections.sort(list)

        var i = 0
        while (i < list.size()) {
          val groupSize = math.min(maxRangesPerGroup, list.size() - i)
          addGroup(list.subList(i, i + groupSize))
          i += groupSize
        }
      }

      if (coprocessor) {
        groupedScans.result
      } else {
        // shuffle the ranges, otherwise our threads will tend to all hit the same region server at once
        Random.shuffle(groupedScans.result)
      }
    }
  }
}

object HBaseIndexAdapter extends LazyLogging {

  private val distributedJarNamePattern = Pattern.compile("^geomesa-hbase-distributed-runtime.*\\.jar$")

  val durability: Durability = HBaseSystemProperties.WalDurability.option match {
    case Some(value) =>
      Durability.values.find(_.toString.equalsIgnoreCase(value)).getOrElse {
        logger.error(s"Invalid HBase WAL durability setting: $value. Falling back to default durability")
        Durability.USE_DEFAULT
      }
    case None => Durability.USE_DEFAULT
  }

  /**
    * Scala convenience method for org.apache.hadoop.hbase.filter.MultiRowRangeFilter#sortAndMerge(java.util.List)
    *
    * @param ranges scan ranges
    * @return
    */
  @deprecated
  def sortAndMerge(ranges: Seq[Scan]): java.util.List[RowRange] = {
    val rowRanges = new java.util.ArrayList[RowRange](ranges.length)
    ranges.foreach(r => rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false)))
    MultiRowRangeFilter.sortAndMerge(rowRanges)
  }

  /**
    * Deserializes row bytes into simple features
    *
    * @param index feature index
    * @param returnSft schema of result rows
    * @return
    */
  def resultsToFeatures(index: GeoMesaFeatureIndex[_, _],
                        returnSft: SimpleFeatureType): CloseableIterator[Result] => CloseableIterator[SimpleFeature] =
    rowsToFeatures(index, KryoFeatureSerializer(returnSft, SerializationOptions.withoutId))

  /**
    * Deserializes row bytes into simple features
    *
    * @param index feature index
    * @param serializer serializer
    * @param rows rows
    * @return
    */
  private def rowsToFeatures(index: GeoMesaFeatureIndex[_, _],
                             serializer: KryoFeatureSerializer)
                            (rows: CloseableIterator[Result]): CloseableIterator[SimpleFeature] = {
    rows.map { row =>
      val cell = row.rawCells()(0)
      val sf = serializer.deserialize(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      val id = index.getIdFromRow(cell.getRowArray, cell.getRowOffset, cell.getRowLength, sf)
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
      sf
    }
  }

  /**
    * Writer for hbase
    *
    * @param ds datastore
    * @param indices indices to write to
    * @param partition partition to write to
    */
  class HBaseIndexWriter(ds: HBaseDataStore,
                         indices: Seq[GeoMesaFeatureIndex[_, _]],
                         wrapper: FeatureWrapper,
                         partition: Option[String]) extends IndexWriter(indices, wrapper) {

    private val batchSize = HBaseSystemProperties.WriteBatchSize.toLong

    private val mutators = indices.toArray.map { index =>
      val table = index.getTableNames(partition) match {
        case Seq(t) => t // should always be writing to a single table here
        case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
      }
      val params = new BufferedMutatorParams(TableName.valueOf(table))
      batchSize.foreach(params.writeBufferSize)
      ds.connection.getBufferedMutator(params)
    }

    private var i = 0

    override protected def write(feature: WritableFeature, values: Array[RowKeyValue[_]], update: Boolean): Unit = {
      if (update) {
        // for updates, ensure that our timestamps don't clobber each other
        flush()
        Thread.sleep(1)
      }
      i = 0
      while (i < values.length) {
        val mutator = mutators(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            kv.values.foreach { value =>
              val put = new Put(kv.row)
              put.addImmutable(value.cf, value.cq, value.value)
              if (!value.vis.isEmpty) {
                put.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
              }
              put.setDurability(durability)
              mutator.mutate(put)
            }

          case mkv: MultiRowKeyValue[_] =>
            mkv.rows.foreach { row =>
              mkv.values.foreach { value =>
                val put = new Put(row)
                put.addImmutable(value.cf, value.cq, value.value)
                if (!value.vis.isEmpty) {
                  put.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
                }
                put.setDurability(durability)
                mutator.mutate(put)
              }
            }
        }
        i += 1
      }
    }

    override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      while (i < values.length) {
        val mutator = mutators(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            kv.values.foreach { value =>
              val del = new Delete(kv.row)
              del.addFamily(value.cf) // note: passing in the column qualifier seems to keep deletes from working
              if (!value.vis.isEmpty) {
                del.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
              }
              mutator.mutate(del)
            }

          case mkv: MultiRowKeyValue[_] =>
            mkv.rows.foreach { row =>
              mkv.values.foreach { value =>
                val del = new Delete(row)
                del.addFamily(value.cf) // note: passing in the column qualifier seems to keep deletes from working
                if (!value.vis.isEmpty) {
                  del.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
                }
                mutator.mutate(del)
              }
            }
        }
        i += 1
      }
    }

    override def flush(): Unit = {
      val exceptions = mutators.flatMap(FlushWithLogging.apply)
      if (exceptions.nonEmpty) {
        val head = exceptions.head
        exceptions.tail.foreach(head.addSuppressed)
        throw head
      }
    }

    override def close(): Unit = {
      val exceptions = mutators.flatMap(CloseWithLogging.apply)
      if (exceptions.nonEmpty) {
        val head = exceptions.head
        exceptions.tail.foreach(head.addSuppressed)
        throw head
      }
    }
  }
}
