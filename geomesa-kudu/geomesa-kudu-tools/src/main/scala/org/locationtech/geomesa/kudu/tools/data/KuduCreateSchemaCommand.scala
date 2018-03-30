/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.data

import com.beust.jcommander.{IValueValidator, Parameter, ParameterException, Parameters}
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.{KuduParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.kudu.tools.data.KuduCreateSchemaCommand.KuduCreateSchemaParams
import org.locationtech.geomesa.tools.data.{CreateSchemaCommand, CreateSchemaParams}
import org.opengis.feature.simple.SimpleFeatureType

class KuduCreateSchemaCommand extends CreateSchemaCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduCreateSchemaParams()

  override protected def setBackendSpecificOptions(featureType: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
    Option(params.compression).foreach { c => featureType.setCompression(c) }
  }
}

object KuduCreateSchemaCommand {
  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class KuduCreateSchemaParams extends CreateSchemaParams with KuduParams with ToggleRemoteFilterParam {
    @Parameter(names = Array("--compression"),
      description = "Enable compression for a feature.  One of \"snappy\", \"lzo\", \"gz\", \"bzip2\", \"lz4\", \"zstd\"", required = false, validateValueWith = classOf[CompressionTypeValidator])
    var compression: String = _
  }

  class CompressionTypeValidator extends IValueValidator[String] {
    val VALID_COMPRESSION_TYPES = Seq("snappy", "lzo", "gz", "bzip2", "lz4", "zstd")

    override def validate(name: String, value: String): Unit = {
      if (!VALID_COMPRESSION_TYPES.contains(value)) {
        throw new ParameterException(s"Invalid compression type.  Values types are ${VALID_COMPRESSION_TYPES.mkString(", ")}")
      }
    }
  }
}
