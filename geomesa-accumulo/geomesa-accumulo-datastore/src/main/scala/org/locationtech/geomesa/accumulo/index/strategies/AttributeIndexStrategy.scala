/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index.strategies

import java.util

import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.accumulo.index.mutable.AttributeTable
import org.locationtech.geomesa.accumulo.index.{AccumuloIndexStrategy, AccumuloMutableIndex}
import org.locationtech.geomesa.api.index.QueryableIndex
import org.opengis.feature.simple.SimpleFeatureType

object AttributeIndexStrategy extends AccumuloIndexStrategy {

  override val name: String = "attr"

  override def supports(sft: SimpleFeatureType) = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    sft.getSchemaVersion > 5 && sft.getAttributeDescriptors.exists(_.isIndexed)
  }

  override def queryable: QueryableIndex[util.Map[Key, Value]] = AttributeIdxxStrategy

  override def mutable: AccumuloMutableIndex = AttributeTable
}
