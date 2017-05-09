/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Polygon
import org.geotools.filter.visitor.BindingFilterVisitor
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.vector.ArrowAttributeReader.{ArrowDateReader, ArrowGeometryReader}
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, ArrowDictionaryReader}
import org.locationtech.geomesa.filter.checkOrderUnsafe
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.spatial.BBOX
import org.opengis.filter.temporal.During
import org.opengis.temporal.Period

import scala.util.control.NonFatal

/**
  * Optimizes filters for running against arrow files
  */
object ArrowFilterOptimizer extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConversions._

  private val ff: FilterFactory2 = new FastFilterFactory

  def rewrite(filter: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = {
    val bound =
      filter.accept(new BindingFilterVisitor(sft), null).asInstanceOf[Filter]
        .accept(new QueryPlanFilterVisitor(sft), ff).asInstanceOf[Filter]
    rewriteFilter(bound, sft, dictionaries)
  }

  private def rewriteFilter(filter: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = {
    try {
      filter match {
        case a: And => ff.and(a.getChildren.map(rewriteFilter(_, sft, dictionaries)))
        case f: BBOX => rewriteBBox(f, sft)
        case f: During => rewriteDuring(f, sft)
        case f: PropertyIsBetween => rewriteBetween(f, sft)
        case f: PropertyIsEqualTo => rewritePropertyIsEqualTo(f, sft, dictionaries)
        case o: Or => ff.or(o.getChildren.map(rewriteFilter(_, sft, dictionaries)))
        case f: Not => ff.not(rewriteFilter(f.getFilter, sft, dictionaries))
        case _ => filter
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Error re-writing filter $filter", e); filter
    }
  }

  private def rewriteBBox(filter: BBOX, sft: SimpleFeatureType): Filter = {
    // TODO handle lines, others?
    if (sft.isPoints) {
      val props = checkOrderUnsafe(filter.getExpression1, filter.getExpression2)
      val bbox = props.literal.evaluate(null).asInstanceOf[Polygon].getEnvelopeInternal
      val attrIndex = sft.indexOf(props.name)
      ArrowPointBBox(attrIndex, bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY)
    } else {
      filter
    }
  }

  private def rewriteDuring(filter: During, sft: SimpleFeatureType): Filter = {
    val props = checkOrderUnsafe(filter.getExpression1, filter.getExpression2)
    val attrIndex = sft.indexOf(props.name)
    val period = props.literal.evaluate(null, classOf[Period])
    val lower = period.getBeginning.getPosition.getDate.getTime
    val upper = period.getEnding.getPosition.getDate.getTime
    ArrowDuring(attrIndex, lower, upper)
  }

  private def rewriteBetween(filter: PropertyIsBetween, sft: SimpleFeatureType): Filter = {
    val attribute = filter.getExpression.asInstanceOf[PropertyName].getPropertyName
    val attrIndex = sft.indexOf(attribute)
    if (sft.getDescriptor(attrIndex).getType.getBinding != classOf[Date]) { filter } else {
      val lower = filter.getLowerBoundary.evaluate(null, classOf[Date]).getTime
      val upper = filter.getUpperBoundary.evaluate(null, classOf[Date]).getTime
      ArrowBetweenDate(attrIndex, lower, upper)
    }
  }

  private def rewritePropertyIsEqualTo(filter: PropertyIsEqualTo,
                                       sft: SimpleFeatureType,
                                       dictionaries: Map[String, ArrowDictionary]): Filter = {
    val props = checkOrderUnsafe(filter.getExpression1, filter.getExpression2)
    // TODO: pass dictionaries around with better attribute names rather than 'actor1Name:String' (requires arrow metadata)
    dictionaries.get(s"${props.name}:String") match {
      case None => filter
      case Some(dictionary) =>
        val attrIndex = sft.indexOf(props.name)
        val numericValue = dictionary.index(props.literal.evaluate(null))
        ArrowDictionaryEquals(attrIndex, numericValue)
    }
  }

  case class ArrowPointBBox(i: Int, xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      val reader = arrow.getReader(i).asInstanceOf[ArrowGeometryReader]
      val y = reader.readPointY(arrow.getIndex)
      if (y >= ymin && y <= ymax) {
        val x = reader.readPointX()
        x >= xmin && x <= xmax
      } else {
        false
      }
    }
  }

  case class ArrowDuring(i: Int, lower: Long, upper: Long) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      val time = arrow.getReader(i).asInstanceOf[ArrowDateReader].getTime(arrow.getIndex)
      // note that during is exclusive
      time > lower && time < upper
    }
  }

  case class ArrowBetweenDate(i: Int, lower: Long, upper: Long) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      val time = arrow.getReader(i).asInstanceOf[ArrowDateReader].getTime(arrow.getIndex)
      // note that during is inclusive
      time >= lower && time <= upper
    }
  }

  case class ArrowDictionaryEquals(i: Int, value: Int) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      arrow.getReader(i).asInstanceOf[ArrowDictionaryReader].getEncoded(arrow.getIndex) == value
    }
  }
}
