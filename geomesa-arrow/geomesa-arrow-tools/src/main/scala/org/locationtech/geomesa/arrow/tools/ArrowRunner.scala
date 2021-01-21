/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.arrow.tools.export.ArrowExportCommand
import org.locationtech.geomesa.arrow.tools.ingest.ArrowIngestCommand
import org.locationtech.geomesa.arrow.tools.stats._
import org.locationtech.geomesa.arrow.tools.status._
import org.locationtech.geomesa.tools.export.GenerateAvroSchemaCommand
import org.locationtech.geomesa.tools.help.HelpCommand
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object ArrowRunner extends Runner {

  override val name: String = "geomesa-arrow"

  override def createCommands(jc: JCommander): Seq[Command] = {
    super.createCommands(jc) ++ Seq(
      new ArrowDescribeSchemaCommand,
      new ArrowExportCommand,
      new ArrowIngestCommand,
      new ArrowGetTypeNamesCommand,
      new ArrowGetSftConfigCommand,
      new GenerateAvroSchemaCommand,
      new ArrowStatsBoundsCommand,
      new ArrowStatsCountCommand,
      new ArrowStatsTopKCommand,
      new ArrowStatsHistogramCommand
    )
  }
}
