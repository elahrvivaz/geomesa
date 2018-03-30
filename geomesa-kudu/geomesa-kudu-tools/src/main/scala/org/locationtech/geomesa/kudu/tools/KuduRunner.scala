/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.kudu.tools.data._
import org.locationtech.geomesa.kudu.tools.export.KuduExportCommand
import org.locationtech.geomesa.kudu.tools.ingest.{KuduBulkIngestCommand, KuduBulkLoadCommand, KuduIngestCommand}
import org.locationtech.geomesa.kudu.tools.stats._
import org.locationtech.geomesa.kudu.tools.status._
import org.locationtech.geomesa.tools.export.GenerateAvroSchemaCommand
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, ConvertCommand, Runner}

object KuduRunner extends Runner {

  override val name: String = "geomesa-kudu"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new KuduBulkIngestCommand,
    new KuduBulkLoadCommand,
    new KuduCreateSchemaCommand,
    new KuduDeleteCatalogCommand,
    new KuduDeleteFeaturesCommand,
    new KuduDescribeSchemaCommand,
    new EnvironmentCommand,
    new KuduExplainCommand,
    new KuduExportCommand,
    new HelpCommand(this, jc),
    new KuduIngestCommand,
    new KuduKeywordsCommand,
    new KuduGetTypeNamesCommand,
    new KuduRemoveSchemaCommand,
    new KuduVersionRemoteCommand,
    new VersionCommand,
    new KuduGetSftConfigCommand,
    new GenerateAvroSchemaCommand,
    new KuduStatsAnalyzeCommand,
    new KuduStatsBoundsCommand,
    new KuduStatsCountCommand,
    new KuduStatsTopKCommand,
    new KuduStatsHistogramCommand,
    new ConvertCommand,
    new ConfigureCommand,
    new ClasspathCommand,
    new ScalaConsoleCommand
  )

  override def environmentErrorInfo(): Option[String] = {
    if (sys.env.get("HBASE_HOME").isEmpty || sys.env.get("HADOOP_HOME").isEmpty) {
      Option("Warning: you have not set HBASE_HOME and/or HADOOP_HOME as environment variables." +
        "\nGeoMesa tools will not run without the appropriate Kudu and Hadoop jars in the tools classpath." +
        "\nPlease ensure that those jars are present in the classpath by running 'geomesa-kudu classpath'." +
        "\nTo take corrective action, please place the necessary jar files in the lib directory of geomesa-tools.")
    } else { None }
  }
}
