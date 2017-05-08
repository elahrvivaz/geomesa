/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.filter.checkOrderUnsafe
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{Filter, FilterVisitor, PropertyIsEqualTo}

import scala.util.control.NonFatal

/**
  * Optimizes filters for running against arrow files
  */
object ArrowFilterOptimizer extends LazyLogging {

  def rewrite(f: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = f match {
    case e: PropertyIsEqualTo => rewritePropertyIsEqualTo(e, sft, dictionaries)
    case _ => f
  }

  private def rewritePropertyIsEqualTo(f: PropertyIsEqualTo,
                                       sft: SimpleFeatureType,
                                       dictionaries: Map[String, ArrowDictionary]): Filter = {
    try {
      val props = checkOrderUnsafe(f.getExpression1, f.getExpression2)
      // TODO: pass dictionaries around with better attribute names rather than 'actor1Name:String' (requires arrow metadata)
      dictionaries.get(s"${props.name}:String") match {
        case None => f
        case Some(dictionary) =>
          val attrIndex = sft.indexOf(props.name)
          val numericValue = dictionary.index(props.literal)
          FastEquals(numericValue, attrIndex)
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Error re-writing filter $f", e); f
    }
  }

  case class FastEquals(v: Int, attrIndex: Int) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean =
      o.asInstanceOf[ArrowSimpleFeature].getAttributeEncoded(attrIndex).asInstanceOf[Int] == v
  }
}
