/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.{Collections, Optional}

import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.fs.storage.api.FilterPartitions
import org.locationtech.geomesa.fs.storage.common.partitions.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.jts.geom.Point
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

class Z2Scheme(bits: Int, geom: String, leaf: Boolean) extends SpatialScheme(bits, geom, leaf) {

  private val z2 = new Z2SFC(bits / 2)

  override def getName: String = Z2Scheme.Name

  override def getPartition(feature: SimpleFeature): String = {
    val pt = feature.getAttribute(geom).asInstanceOf[Point]
    z2.index(pt.getX, pt.getY).z.formatted(format)
  }

  override protected def digits(bits: Int): Int = math.ceil(bits * math.log10(2)).toInt

  override protected def generateRanges(xy: Seq[(Double, Double, Double, Double)]): Seq[IndexRange] = z2.ranges(xy)

//  override def getPartitions(filter: Filter): Optional[java.util.List[FilterPartitions]] = {
//
//  }
//  override def getPartitionsForQuery(filter: Filter, simplify: Boolean): Optional[java.util.List[FilterPartitions]] = {
//    val bounds = FilterHelper.extractGeometries(filter, geom, intersect = true)
//    if (bounds.disjoint) {
//      Optional.of(Collections.emptyList())
//    } else if (bounds.isEmpty) {
//      Optional.empty()
//    } else {
//      val covered = new java.util.ArrayList[String]()
//      val partial = new java.util.ArrayList[String]()
//
//      ranges(bounds.values).foreach { range =>
//        if (range.contained) {
//          partitions(Seq(range)).foreach(covered.add)
//        } else {
//          partitions(Seq(range)).foreach(partial.add)
//        }
//      }
//
//      // if any ranges were fully covered by one bounds and partially by another, deduplicate them
//      partial.removeAll(covered)
//
//      def coveredFilter: Filter = {
//        import org.locationtech.geomesa.filter.isSpatialFilter
//        FilterExtractingVisitor(filter, geom, isSpatialFilter _)._2.getOrElse(Filter.INCLUDE)
//      }
//
//      if (covered.isEmpty && partial.isEmpty) {
//        Optional.of(Collections.emptyList()) // equivalent to Filter.EXCLUDE
//      } else if (covered.isEmpty) {
//        Optional.of(Collections.singletonList(new FilterPartitions(filter, partial)))
//      } else if (partial.isEmpty) {
//        Optional.of(Collections.singletonList(new FilterPartitions(coveredFilter, covered)))
//      } else {
//        val filterPartitions = new java.util.ArrayList[FilterPartitions](2)
//        filterPartitions.add(new FilterPartitions(coveredFilter, covered))
//        filterPartitions.add(new FilterPartitions(filter, partial))
//        Optional.of(filterPartitions)
//      }
//    }
//  }
}

object Z2Scheme {

  val Name = "z2"

  class Z2PartitionSchemeFactory extends SpatialPartitionSchemeFactory {
    override val Name: String = Z2Scheme.Name
    override def buildPartitionScheme(bits: Int, geom: String, leaf: Boolean): SpatialScheme =
      new Z2Scheme(bits, geom, leaf)
  }
}
