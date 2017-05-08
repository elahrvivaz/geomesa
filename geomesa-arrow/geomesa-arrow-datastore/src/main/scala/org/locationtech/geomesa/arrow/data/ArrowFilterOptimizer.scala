/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.visitor.BindingFilterVisitor
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.filter.checkOrderUnsafe
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.util.control.NonFatal

/**
  * Optimizes filters for running against arrow files
  */
object ArrowFilterOptimizer extends LazyLogging {

  import scala.collection.JavaConversions._

  private val ff: FilterFactory2 = new FastFilterFactory

  def rewrite(filter: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = {
    val bound =
      filter.accept(new BindingFilterVisitor(sft), null).asInstanceOf[Filter]
        .accept(new QueryPlanFilterVisitor(sft), ff).asInstanceOf[Filter]
    _rewrite(bound, sft, dictionaries)
  }

  private def _rewrite(filter: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = {
    filter match {
      case a: And => ff.and(a.getChildren.map(_rewrite(_, sft, dictionaries)))
      case o: Or => ff.or(o.getChildren.map(_rewrite(_, sft, dictionaries)))
      case f: PropertyIsEqualTo => rewritePropertyIsEqualTo(f, sft, dictionaries)
      case f: Not => ff.not(_rewrite(f.getFilter, sft, dictionaries))
      case _ => filter
    }
  }

  private def rewritePropertyIsEqualTo(filter: PropertyIsEqualTo,
                                       sft: SimpleFeatureType,
                                       dictionaries: Map[String, ArrowDictionary]): Filter = {
    try {
      val props = checkOrderUnsafe(filter.getExpression1, filter.getExpression2)
      // TODO: pass dictionaries around with better attribute names rather than 'actor1Name:String' (requires arrow metadata)
      dictionaries.get(s"${props.name}:String") match {
        case None => filter
        case Some(dictionary) =>
          val attrIndex = sft.indexOf(props.name)
          val numericValue = dictionary.index(props.literal.evaluate(null))
          EncodedDictionaryEquals(numericValue, attrIndex)
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Error re-writing filter $filter", e); filter
    }
  }

  case class EncodedDictionaryEquals(v: Int, attrIndex: Int) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = o.asInstanceOf[ArrowSimpleFeature].getAttributeEncoded(attrIndex) == v
  }
}
