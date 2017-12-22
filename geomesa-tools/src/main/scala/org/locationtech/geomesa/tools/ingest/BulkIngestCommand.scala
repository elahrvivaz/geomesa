/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.config.Config
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.BulkIngestCommand.BulkIngestParams
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.io.PathUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

trait BulkIngestCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  import scala.collection.JavaConversions._

  override val name = "ingest-bulk"
  override def params: BulkIngestParams

  def libjarsFile: String
  def libjarsPaths: Iterator[() => Seq[File]]

  override def execute(): Unit = {
    ensureSameFs(PathUtils.RemotePrefixes)
    if (!params.files.headOption.exists(PathUtils.isRemote)) {
      throw new ParameterException("Bulk ingest requires input files to be staged in a distributed file system (e.g. HDFS)")
    }

    val converterConfig = CLArgResolver.getConfig(params.config)

    val sft = if (params.spec != null) {
      CLArgResolver.getSft(params.spec, params.featureName)
    } else if (params.featureName != null) {
      Try(withDataStore(_.getSchema(params.featureName))).filter(_ != null).getOrElse {
        throw new ParameterException(s"SimpleFeatureType '${params.featureName}' does not currently exist, " +
            "please provide specification argument")
      }
    } else {
      throw new ParameterException("SimpleFeatureType name and/or specification argument is required")
    }

    val ingest = new ConverterIngest(sft, connection, converterConfig, params.files, Some(RunModes.Distributed),
      libjarsFile, libjarsPaths, 0) {

      override def runDistributedJob(statusCallback: StatusCallback): (Long, Long) = {
        val job = createIngestJob(sft, converterConfig)
        job.run(dsParams, sft.getTypeName, params.files, params.reducers, Option(params.tempPath),
          libjarsFile, libjarsPaths, statusCallback)
      }
    }
    ingest.run()
  }

  protected def createIngestJob(sft: SimpleFeatureType, converterConfig: Config): ConverterBulkIngestJob[_, _]

  def ensureSameFs(prefixes: Seq[String]): Unit = {
    prefixes.foreach { pre =>
      if (params.files.exists(_.toLowerCase.startsWith(s"$pre://")) &&
        !params.files.forall(_.toLowerCase.startsWith(s"$pre://"))) {
        throw new ParameterException(s"Files must all be on the same file system: ($pre) or all be local")
      }
    }
  }
}

object BulkIngestCommand {
  // @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  trait BulkIngestParams extends OptionalTypeNameParam with OptionalFeatureSpecParam
      with RequiredConverterConfigParam with OptionalInputFormatParam {
    // TODO extract to common params
    @Parameter(names = Array("--num-reducers"), description = "Number of reducers", required = true)
    var reducers: java.lang.Integer = _

    @Parameter(names = Array("--temp-path"), description = "Path to temp directory for bulk output. " +
        "May be useful when writing to s3 since it is slow as a sink", required = false)
    var tempPath: String = _
  }
}
