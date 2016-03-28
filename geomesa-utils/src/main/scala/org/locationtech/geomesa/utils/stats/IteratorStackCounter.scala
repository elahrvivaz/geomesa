/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

/**
 * The IteratorStackCounter keeps track of the number of times Accumulo sets up an iterator stack
 * as a result of a query.
 */
class IteratorStackCounter extends Stat {

  private [stats] var cnt: Long = 1

  override type S = IteratorStackCounter

  def count: Long = cnt

  override def observe(sf: SimpleFeature): Unit = {}

  override def +(other: IteratorStackCounter): IteratorStackCounter = {
    val plus = new IteratorStackCounter()
    plus.cnt += this.cnt
    plus.cnt += other.cnt
    plus
  }

  override def +=(other: IteratorStackCounter): Unit = cnt += other.cnt

  override def toJson(): String = s"""{ "count": $cnt }"""

  override def isEmpty: Boolean = false

  override def clear(): Unit = cnt = 1L

  override def equals(other: Any): Boolean = other match {
    case that: IteratorStackCounter => cnt == that.cnt
    case _ => false
  }

  override def hashCode(): Int = cnt.hashCode
}
