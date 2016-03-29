/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

/**
  * Counts features
  */
class CountStat() extends Stat {

  override type S = CountStat

  var count: Long = 0L

  override def observe(sf: SimpleFeature): Unit = count += 1

  override def +(other: CountStat): CountStat = {
    val plus = new CountStat()
    plus.count = this.count + other.count
    plus
  }

  override def +=(other: CountStat): Unit = count += other.count

  override def toJson(): String = s"""{ "count": $count }"""

  override def isEmpty: Boolean = count == 0

  override def clear(): Unit = count = 0

  override def equals(other: Any): Boolean = other match {
    case that: CountStat => count == that.count
    case _ => false
  }

  override def hashCode(): Int = count.hashCode
}