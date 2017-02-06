/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, Parameters}
import org.apache.accumulo.core.client.Connector
import org.geotools.jdbc.JDBCDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams, RequiredCredentialsParams}
import org.locationtech.geomesa.tools.ingest.{IngestCommand, IngestParams}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

class AccumuloIngestCommand extends IngestCommand[AccumuloDataStore] with AccumuloDataStoreCommand {

  override val params = new AccumuloIngestParams()

  override val libjarsFile: String = "org/locationtech/geomesa/accumulo/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_ACCUMULO_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[Connector])
  )
}

@Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
class AccumuloIngestParams extends IngestParams with AccumuloDataStoreParams

class PostgisIngestCommand extends IngestCommand[JDBCDataStore] {

  override val name = "postgis-ingest"
  override val params = new PostgisIngestParams()

  override val libjarsFile: String = "org/locationtech/geomesa/accumulo/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_ACCUMULO_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[Connector])
  )

  override def connection: Map[String, String] = {
    Map(
      "dbtype"   -> "postgis",
      "host"     -> params.host,
      "port"     -> params.port.toString,
      "schema"   -> params.schema,
      "database" -> params.db,
      "user"     -> params.user,
      "passwd"   -> params.password)
  }
}

@Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
class PostgisIngestParams extends IngestParams with RequiredCredentialsParams {
  @Parameter(names = Array("--database"), description = "Database name", required = true)
  var db: String = null
  @Parameter(names = Array("--schema"), description = "Schema name", required = true)
  var schema: String = null
  @Parameter(names = Array("--port"), description = "Port", required = true)
  var port: Int = 5432
  @Parameter(names = Array("--host"), description = "Server name", required = true)
  var host: String = null

}
