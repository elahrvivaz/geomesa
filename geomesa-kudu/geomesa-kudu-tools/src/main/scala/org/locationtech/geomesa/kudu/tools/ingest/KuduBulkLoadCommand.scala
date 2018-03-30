/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.kudu.TableName
import org.apache.hadoop.kudu.mapreduce.LoadIncrementalHFiles
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.{KuduParams, RemoteFilterNotUsedParam}
import org.locationtech.geomesa.kudu.tools.ingest.KuduBulkLoadCommand.BulkLoadParams
import org.locationtech.geomesa.tools.{Command, RequiredIndexParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.text.TextTools

class KuduBulkLoadCommand extends KuduDataStoreCommand {

  override val name: String = "bulk-load"
  override val params = new BulkLoadParams

  override def execute(): Unit = withDataStore(run)

  def run(ds: KuduDataStore): Unit = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist")
    }

    val index = params.loadRequiredIndex(ds, IndexMode.Write).asInstanceOf[KuduFeatureIndex]
    val input = new Path(params.input)

    Command.user.info(s"Running Kudu incremental load...")
    val start = System.currentTimeMillis()
    val tableName = TableName.valueOf(index.getTableName(params.featureName, ds))
    val table = ds.connection.getTable(tableName)
    val locator = ds.connection.getRegionLocator(tableName)
    val config = new Configuration
    config.set("kudu.loadincremental.validate.hfile", params.validate.toString)
    new LoadIncrementalHFiles(config).doBulkLoad(input, ds.connection.getAdmin, table, locator)
    Command.user.info(s"Kudu incremental load complete in ${TextTools.getTime(start)}")
  }
}

object KuduBulkLoadCommand {
  @Parameters(commandDescription = "Bulk load HFiles into Kudu")
  class BulkLoadParams extends KuduParams
      with RequiredTypeNameParam with RequiredIndexParam with RemoteFilterNotUsedParam {
    @Parameter(names = Array("--input"), description = "Path to HFiles to be loaded", required = true)
    var input: String = _

    @Parameter(names = Array("--validate"), description = "Validate HFiles before loading")
    var validate: Boolean = true
  }
}
