/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.regex.Pattern
import java.util.{Collections, Optional}

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.SpatialScheme.Config
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.jts.geom.Geometry
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

abstract class SpatialScheme(bits: Int, geom: String, leaf: Boolean) extends PartitionScheme {

  import scala.collection.JavaConverters._

  require(bits % 2 == 0, "Resolution must be an even number")

  protected val format = s"%0${digits(bits)}d"

  protected def digits(bits: Int): Int

  protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange]

  protected def ranges(bounds: Seq[Geometry]): Seq[IndexRange] = {
    val geoms = if (bounds.isEmpty) { Seq(WholeWorldPolygon) } else { bounds }
    generateRanges(geoms.map(GeometryUtils.bounds))
  }

  protected def partitions(ranges: Seq[IndexRange]): Seq[String] =
    ranges.flatMap(r => r.lower to r.upper).distinct.map(_.formatted(format))

  override def getPartitions(filter: Filter): java.util.List[String] = {
    val geometries = FilterHelper.extractGeometries(filter, geom, intersect = true)
    if (geometries.disjoint) {
      Collections.emptyList()
    } else {
      partitions(ranges(geometries.values)).asJava
    }
  }

  override def getOptions: java.util.Map[String, String] = {
    Map(
      Config.GeomAttribute          -> geom,
      Config.resolutionOption(this) -> bits.toString,
      Config.LeafStorage            -> leaf.toString
    ).asJava
  }

  override def getMaxDepth: Int = 1

  override def isLeafStorage: Boolean = leaf

  override def equals(other: Any): Boolean = other match {
    case that: Z2Scheme => that.getOptions.equals(getOptions)
    case _ => false
  }

  override def hashCode(): Int = getOptions.hashCode()
}

object SpatialScheme {

  object Config {
    val GeomAttribute: String = "geom-attribute"
    val LeafStorage  : String = LeafStorageConfig

    def resolutionOption(scheme: SpatialScheme): String = s"${scheme.getName}-resolution"
  }

  trait SpatialPartitionSchemeFactory extends PartitionSchemeFactory {

    def Name: String

    lazy val NamePattern: Pattern = Pattern.compile(s"$Name(-([0-9]+)bits?)?")
    lazy val Resolution = s"$Name-resolution"


    override def load(name: String,
                      sft: SimpleFeatureType,
                      options: java.util.Map[String, String]): Optional[PartitionScheme] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val matcher = NamePattern.matcher(name)
      if (!matcher.matches()) { Optional.empty() } else {
        val geom = Option(options.get(Config.GeomAttribute)).getOrElse(sft.getGeomField)
        if (sft.indexOf(geom) == -1) {
          throw new IllegalArgumentException(s"$Name scheme requires valid geometry field '${Config.GeomAttribute}'")
        }
        val res =
          Option(matcher.group(2))
              .filterNot(_.isEmpty)
              .orElse(Option(options.get(Resolution)))
              .map(Integer.parseInt)
              .getOrElse {
                throw new IllegalArgumentException(s"$Name scheme requires bit resolution '$Resolution'")
              }
        val leaf = Option(options.get(Config.LeafStorage)).forall(java.lang.Boolean.parseBoolean)
        Optional.of(buildPartitionScheme(res, geom, leaf))
      }
    }

    def buildPartitionScheme(bits: Int, geom: String, leaf: Boolean): SpatialScheme
  }
}
