/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.ingest

import com.beust.jcommander.Parameters
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.jobs.KuduIndexFileMapper
import org.locationtech.geomesa.kudu.tools.ingest.KuduBulkIngestCommand.KuduBulkIngestParams
import org.locationtech.geomesa.kudu.tools.ingest.KuduIngestCommand.KuduIngestParams
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.{ConverterIngest, ConverterIngestJob}
import org.locationtech.geomesa.tools.{Command, OutputPathParam, RequiredIndexParam}
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class KuduBulkIngestCommand extends KuduIngestCommand {

  override val name = "bulk-ingest"
  override val params = new KuduBulkIngestParams()

  override protected def createConverterIngest(sft: SimpleFeatureType, converterConfig: Config): Runnable = {
    import scala.collection.JavaConverters._

    new ConverterIngest(sft, connection, converterConfig, params.files.asScala, Option(params.mode),
      libjarsFile, libjarsPaths, params.threads) {

      override def run(): Unit = {
        super.run()
        Command.user.info("To load files, run:\n\tgeomesa-kudu bulk-load " +
            s"-c ${params.catalog} -f ${sft.getTypeName} --index ${params.index} --input ${params.outputPath}")
      }

      override def runDistributedJob(statusCallback: StatusCallback): (Long, Long) = {
        // validate index param now that we have a datastore and the sft has been created
        val index = params.loadRequiredIndex(ds.asInstanceOf[KuduDataStore], IndexMode.Write).identifier
        val job = new ConverterIngestJob(dsParams, sft, converterConfig, inputs, libjarsFile, libjarsPaths) {
          override def configureJob(job: Job): Unit = {
            super.configureJob(job)
            KuduIndexFileMapper.configure(job, connection, sft.getTypeName, index, new Path(params.outputPath))
          }
        }
        job.run(statusCallback)
      }

      override protected def runLocal(): Unit =
        throw new NotImplementedError("Bulk ingest not implemented for local mode")
    }
  }

  override protected def createAutoIngest(): Runnable =
    throw new NotImplementedError("Bulk auto ingest not implemented")

  override protected def createShpIngest(): Runnable =
    throw new NotImplementedError("Bulk ShpFile ingest not implemented")
}

object KuduBulkIngestCommand {
  @Parameters(commandDescription = "Convert various file formats into Kudu HFiles suitable for incremental load")
  class KuduBulkIngestParams extends KuduIngestParams with RequiredIndexParam with OutputPathParam
}
