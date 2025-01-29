/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.{And, Filter, Or, PropertyIsEqualTo}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

/**
 * Enumerated scheme, for a fixed set of partition names. Reads will go to all enumerations, and writes will go
 * to a particular enumeration based on a hash of the feature id.
 *
 * @param enumerations list of fixed partition segments
 */
case class EnumeratedScheme(enumerations: IndexedSeq[String]) extends PartitionScheme {

  import FilterHelper.ff

  override val depth: Int = 1

  override def pattern: String = "<enum>"

  override def getPartitionName(feature: SimpleFeature): String = enumerations(feature.getID.hashCode % enumerations.length)

  override def getSimplifiedFilters(filter: Filter, partition: Option[String]): Option[Seq[SimplifiedFilter]] = {
    partition match {
      case None => Some(Seq(SimplifiedFilter(filter, enumerations, partial = false)))
      case Some(p) => Some(Seq(SimplifiedFilter(filter, Seq(p), partial = false)))
    }
  }

  override def getIntersectingPartitions(filter: Filter): Option[Seq[String]] = Some(enumerations)

  // TODO there isn't any hashcode function...
  override def getCoveringFilter(partition: String): Filter = throw new NotImplementedError()

}

object EnumeratedScheme {

  val Name = "attribute"

  /**
    * Check to extract only the equality filters that we can process with this partition scheme
    *
    * @param filter filter
    * @return
    */
  def propertyIsEquals(filter: Filter): Boolean = {
    filter match {
      case _: And | _: Or => true // note: implies further processing of children
      case _: PropertyIsEqualTo => true
      case _ => false
    }
  }

  object Config {
    val AttributeOpt: String = "partitioned-attribute"
  }

  class AttributePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme] = {
      if (config.name != Name) { None } else {
        val attribute = config.options.getOrElse(Config.AttributeOpt, null)
        require(attribute != null, s"Attribute scheme requires valid attribute name '${Config.AttributeOpt}'")
        val index = sft.indexOf(attribute)
        require(index != -1, s"Attribute '$attribute' does not exist in schema '${sft.getTypeName}'")
        val binding = sft.getDescriptor(index).getType.getBinding
        require(AttributeIndexKey.encodable(binding),
          s"Invalid type binding '${binding.getName}' of attribute '$attribute'")
        Some(AttributeScheme(attribute, index, binding))
      }
    }
  }
}
