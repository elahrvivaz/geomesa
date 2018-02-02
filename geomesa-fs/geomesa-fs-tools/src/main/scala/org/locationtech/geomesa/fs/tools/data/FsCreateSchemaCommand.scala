/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.data

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.conf.{PartitionSchemeArgResolver, SchemeArgs}
import org.locationtech.geomesa.fs.storage.common.{PartitionOpts, PartitionScheme}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, SchemeParams}
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand.FsCreateSchemaParams
import org.locationtech.geomesa.tools.data.{CreateSchemaCommand, CreateSchemaParams}
import org.opengis.feature.simple.SimpleFeatureType

class FsCreateSchemaCommand extends CreateSchemaCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params: FsCreateSchemaParams = new FsCreateSchemaParams

  override protected def setBackendSpecificOptions(sft: SimpleFeatureType): Unit =
    FsCreateSchemaCommand.setPartitionScheme(sft, params)
}

object FsCreateSchemaCommand {

  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class FsCreateSchemaParams extends CreateSchemaParams with FsParams with SchemeParams

  def setPartitionScheme(sft: SimpleFeatureType, params: SchemeParams): Unit = {
    import scala.collection.JavaConverters._

    val scheme = PartitionSchemeArgResolver.getArg(SchemeArgs(params.scheme, sft)) match {
      case Left(e) => throw new ParameterException(e)
      case Right(s) if s.isLeafStorage == params.leafStorage => s
      case Right(s) =>
        val opts = s.getOptions.asScala.updated(PartitionOpts.LeafStorage, params.leafStorage.toString).toMap
        PartitionScheme.apply(sft, s.name(), opts)
    }

    PartitionScheme.addToSft(sft, scheme)

    // Can use this to set things like compression and summary levels for parquet in the sft user data
    // to be picked up by the ingest job
    params.storageOpts.asScala.foreach { case (k, v) => sft.getUserData.put(k,v) }
  }
}