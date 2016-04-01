/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.commands

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Duration
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaMetadataStats
import org.locationtech.geomesa.tools.commands.StatsListCommand.StatsListParameters
import org.locationtech.geomesa.utils.stats.{MinMax, RangeHistogram, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class StatsListCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  import StatsListCommand.{getAttributes, histToString, minMaxToString}

  override val command = "list-stats"
  override val params = new StatsListParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val attributes = getAttributes(sft, params)

    if (params.exact) {
      attributes.foreach(a => printExactStat(sft, a, filter))
    } else {
      attributes.foreach(a => printExistingStat(sft, a, filter))
    }
  }

  private def printExistingStat(sft: SimpleFeatureType, attribute: String, filter: Filter): Unit = {
    val minMax = ds.stats.getMinMax[Any](sft, attribute, filter)
    if (minMax.isEmpty) {
      System.err.println(s"No stats available for attribute '$attribute'")
    } else {
      val histogram = ds.stats.getHistogram[Any](sft, attribute).map(histToString(attribute, _))
      // histogram and bounds might differ - print them both
      println(minMaxToString(attribute, minMax))
      histogram.foreach(println)
    }
  }

  private def printExactStat(sft: SimpleFeatureType, attribute: String, filter: Filter): Unit = {
    System.err.println(s"Calculating stats for '$attribute'...")
    val minMax = ds.stats.getMinMax[Any](sft, attribute, filter, exact = true)
    minMax.bounds match {
      case None => System.err.println(s"No stats available for attribute '$attribute'")
      case Some(bounds) =>
        val histString = bounds match {
          case (min: String,  max: String)  => Stat.RangeHistogram(attribute, 10, min, max)
          case (min: Integer, max: Integer) => Stat.RangeHistogram(attribute, 10, min, max)
          case (min: jLong,   max: jLong)   => Stat.RangeHistogram(attribute, 10, min, max)
          case (min: jFloat,  max: jFloat)  => Stat.RangeHistogram(attribute, 10, min, max)
          case (min: jDouble, max: jDouble) => Stat.RangeHistogram(attribute, 10, min, max)

          case (min: Geometry, max: Geometry) =>
            Stat.RangeHistogram(attribute, GeoMesaMetadataStats.GeometryHistogramSize, min, max)

          case (min: Date, max: Date) =>
            val days = new Duration(min.getTime, max.getTime).getStandardDays
            val bins = if (days < 14) days else days / 7
            Stat.RangeHistogram(attribute, bins.toInt, min, max)

          case _ => throw new NotImplementedError(s"Unexpected min max values $bounds")
        }

        val histogram = ds.stats.runStatQuery[RangeHistogram[Any]](sft, histString, filter)
        // histogram includes the bounds
        println(histToString(attribute, histogram))
    }
  }
}

object StatsListCommand {
  @Parameters(commandDescription = "View statistics on a GeoMesa feature type")
  class StatsListParameters extends OptionalCqlFilterParameters with HasAttributesParam {
    @Parameter(names = Array("-e", "--exact"), description = "Calculate exact statistics (may be slow)")
    var exact: Boolean = false
  }

  def minMaxToString(attribute: String, stat: MinMax[Any]): String =
    s"Bounds for '$attribute': ${stat.toJson}"

  // TODO print out geometry histogram in a more meaningful way
  def histToString(attribute: String, stat: RangeHistogram[Any]): String =
    s"Binned Histogram for '$attribute': ${stat.toJson}"

  def getAttributes(sft: SimpleFeatureType, params: StatsListParameters): Seq[String] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val allAttributes: Seq[String] = Option(params.attributes).map(_.split(",")) match {
      case None => sft.getAttributeDescriptors.map(_.getLocalName)
      case Some(a) => a.map(_.trim)
    }
    allAttributes.filter { a =>
      val ad = sft.getDescriptor(a)
      // TODO support list/map types in stats
      if (ad.isMultiValued || ad.getType.getBinding == classOf[java.lang.Boolean]) {
        System.err.println(s"Ignoring attribute '$a' of unsupported type ${ad.getType.getBinding.getSimpleName}")
        false
      } else {
        true
      }
    }
  }
}
