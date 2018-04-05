/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import org.geotools.filter.expression.PropertyAccessor
import org.geotools.util.Converters
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Expression, ExpressionVisitor, PropertyName}
import org.xml.sax.helpers.NamespaceSupport

/**
 * Implementation of property name that looks up the value by index
 */
class FastPropertyName(name: String, index: Int) extends PropertyName with Expression {

  override def getPropertyName: String = name

  override def getNamespaceContext: NamespaceSupport = null

  override def evaluate(obj: AnyRef): AnyRef = {
    try { obj.asInstanceOf[SimpleFeature].getAttribute(index) } catch {
      case _: ClassCastException =>
        obj match {
          case s: SimpleFeatureType => s.getDescriptor(name)
          case _ => null
        }
    }
  }

  override def evaluate[T](obj: AnyRef, target: Class[T]): T = Converters.convert(evaluate(obj), target)

  override def accept(visitor: ExpressionVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)

  // required for some ECQL parsing
  override def toString: String = name

  override def equals(other: Any): Boolean = other match {
    case that: PropertyName => name == that.getPropertyName
    case _ => false
  }

  override def hashCode(): Int = Seq(name).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
}

class FastPropertyNameAccessor(name: String, accessor: PropertyAccessor) extends PropertyName with Expression {

  override def getPropertyName: String = name

  override def getNamespaceContext: NamespaceSupport = null

  override def evaluate(obj: AnyRef): AnyRef = accessor.get(obj, name, classOf[AnyRef])

  override def evaluate[T](obj: AnyRef, target: Class[T]): T = Converters.convert(evaluate(obj), target)

  override def accept(visitor: ExpressionVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)

  // required for some ECQL parsing
  override def toString: String = name
}