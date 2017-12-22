/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.ingest

import java.io.File

import com.beust.jcommander.Parameters
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.ingest.HBaseBulkIngestCommand.HBaseBulkIngestParams
import org.locationtech.geomesa.tools.CatalogParam
import org.locationtech.geomesa.tools.ingest.BulkIngestCommand.BulkIngestParams
import org.locationtech.geomesa.tools.ingest.{BulkIngestCommand, BulkIngestConverterJob}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class HBaseBulkIngestCommand extends BulkIngestCommand[HBaseDataStore] with HBaseDataStoreCommand with HBaseLibJars {

  override val params = new HBaseBulkIngestParams

  override protected def createIngestJob(sft: SimpleFeatureType,
                                         converterConfig: Config): BulkIngestConverterJob[_, _] = {

  }


}

object HBaseBulkIngestCommand {
  @Parameters(commandDescription = "Bulk ingest/convert various file formats into GeoMesa")
  class HBaseBulkIngestParams extends BulkIngestParams with CatalogParam

  class HBaseBulkIngestConverterJob(sft: SimpleFeatureType, converterConfig: Config)
      extends BulkIngestConverterJob[ImmutableBytesWritable, Cell](sft, converterConfig) {

    override def mapper: Class[_ <: Mapper[LongWritable, SimpleFeature, ImmutableBytesWritable, Cell]] =
      classOf[HBaseBulkIngestMapper]

    override def configureOutput(job: Job, output: String): Unit = {
      FileOutputFormat.setOutputPath(job, new Path(output))
      // TODO this doesn't exist until hbase 2.0.0 ...
      MultiTableHFileOutputFormat.configureIncrementalLoadMap(job, )
//      job.setOutputFormatClass(classOf[MultiTableHFileOutputFormat])
    }

    override def notifyBulkLoad(output: String): Unit = {

    }
  }

  class HBaseBulkIngestMapper extends Mapper[LongWritable, SimpleFeature, ImmutableBytesWritable, Cell] {

  }
}