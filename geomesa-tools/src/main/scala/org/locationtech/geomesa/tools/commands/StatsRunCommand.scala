/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.commands.StatsRunCommand.StatsRunParameters
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType

class StatsRunCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  override val command = "analyze-stats"
  override val params = new StatsRunParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    System.err.println(s"Analyzing stats for feature type ${sft.getTypeName}...")
    val stats = ds.stats.runStats(sft)
    System.err.println(s"Stats analyzed. Results:")
    print(sft, stats)
  }

  private def print(sft: SimpleFeatureType, stat: Stat): Unit = {
    import StatsListCommand.{histToString, minMaxToString}
    import StatsRunCommand.attribute
    stat match {
      case s: MinMax[Any]         => println(minMaxToString(attribute(sft, s.attribute), s))
      case s: RangeHistogram[Any] => println(histToString(attribute(sft, s.attribute), s))
      case s: CountStat           => println(s"Total feature count: ${s.count}")
      case s: SeqStat             => s.stats.foreach(println(sft, _))
      case _ =>
        // corresponds to what we expect in GeoMesaMetadataStats
        throw new NotImplementedError("Only Count, MinMax and RangeHistogram stats are supported")
    }
  }
}

object StatsRunCommand {
  @Parameters(commandDescription = "Analyze statistics on a GeoMesa feature type")
  class StatsRunParameters extends FeatureParams

  private def attribute(sft: SimpleFeatureType, index: Int): String = sft.getDescriptor(index).getLocalName
}
