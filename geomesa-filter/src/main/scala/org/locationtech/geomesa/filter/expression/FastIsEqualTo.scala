/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.filter.expression

import org.opengis.filter.expression.Expression
import org.opengis.filter.{FilterVisitor, MultiValuedFilter, PropertyIsEqualTo}

class FastIsEqualTo(exp1: Expression, exp2: Expression) extends PropertyIsEqualTo {

  override def evaluate(obj: scala.Any): Boolean = exp1.evaluate(obj) == exp2.evaluate(obj)

  override def accept(visitor: FilterVisitor, extraData: scala.Any): AnyRef = visitor.visit(this, extraData)

  override def getExpression1: Expression = exp1

  override def getExpression2: Expression = exp2

  override def isMatchingCase: Boolean = false

  override def getMatchAction: MultiValuedFilter.MatchAction = MultiValuedFilter.MatchAction.ANY
}
