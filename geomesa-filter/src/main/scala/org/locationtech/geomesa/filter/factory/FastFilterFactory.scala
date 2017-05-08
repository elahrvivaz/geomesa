/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.factory

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.expression.{FastIsEqualTo, FastPropertyName}
import org.opengis.feature.`type`.Name
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.{Filter, FilterFactory2, MultiValuedFilter, PropertyIsEqualTo}
import org.xml.sax.helpers.NamespaceSupport

/**
 * Filter factory that overrides property name
 */
class FastFilterFactory extends org.geotools.filter.FilterFactoryImpl with FilterFactory2 {

  override def property(name: String): PropertyName = new FastPropertyName(name)

  override def property(name: Name): PropertyName = property(name.getLocalPart)

  override def property(name: String, namespaceContext: NamespaceSupport): PropertyName = property(name)

  override def equals(exp1: Expression, exp2: Expression): PropertyIsEqualTo = new FastIsEqualTo(exp1, exp2)

  override def equal(exp1: Expression, exp2: Expression, matchCase: Boolean): PropertyIsEqualTo = {
    if (matchCase) {
      new FastIsEqualTo(exp1, exp2)
    } else {
      super.equal(exp1, exp2, matchCase)
    }
  }

  override def equal(exp1: Expression,
                     exp2: Expression,
                     matchCase: Boolean,
                     matchAction: MultiValuedFilter.MatchAction): PropertyIsEqualTo = {
    if (matchCase && matchAction == MultiValuedFilter.MatchAction.ANY) {
      new FastIsEqualTo(exp1, exp2)
    } else {
      super.equal(exp1, exp2, matchCase, matchAction)
    }
  }
}

object FastFilterFactory {

  val factory = new FastFilterFactory

  def toFilter(ecql: String): Filter = ECQL.toFilter(ecql, factory)
}