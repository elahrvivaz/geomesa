/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.ingest.HBaseBulkLoadCommand.BulkLoadParams
import org.locationtech.geomesa.tools.{CatalogParam, Command, RequiredIndexParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.text.TextTools

class HBaseBulkLoadCommand extends HBaseDataStoreCommand {

  override val name: String = "bulk-load"
  override val params = new BulkLoadParams

  override def execute(): Unit = withDataStore(run)

  def run(ds: HBaseDataStore): Unit = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist")
    }

    val index = params.loadRequiredIndex(ds, IndexMode.Write).asInstanceOf[HBaseFeatureIndex]
    val input = new Path(params.input)

    Command.user.info(s"Running HBase incremental load...")
    val start = System.currentTimeMillis()
    val tableName = TableName.valueOf(index.getTableName(params.featureName, ds))
    val table = ds.connection.getTable(tableName)
    val locator = ds.connection.getRegionLocator(tableName)
    val config = new Configuration
    config.set("hbase.loadincremental.validate.hfile", params.validate.toString)
    new LoadIncrementalHFiles(config).doBulkLoad(input, ds.connection.getAdmin, table, locator)
    Command.user.info(s"HBase incremental load complete in ${TextTools.getTime(start)}")
  }
}

object HBaseBulkLoadCommand {
  @Parameters(commandDescription = "Bulk load HFiles into HBase")
  class BulkLoadParams extends CatalogParam with RequiredTypeNameParam with RequiredIndexParam {
    @Parameter(names = Array("--input"), description = "Path to HFiles to be loaded", required = true)
    var input: String = _

    @Parameter(names = Array("--validate"), description = "Validate HFiles before loading")
    var validate: Boolean = true
  }
}
