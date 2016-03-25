/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.stats

import org.geotools.filter.text.ecql.ECQL
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

/**
  * Counts features that match the filter
  *
  * @param ecql filter
  */
class CountStat(val ecql: String) extends Stat {

  override type S = CountStat

  var count: Long = 0L
  private val filter: Filter = ECQL.toFilter(ecql)

  override def observe(sf: SimpleFeature): Unit = {
    if (filter.evaluate(sf)) {
      count += 1
    }
  }

  override def +(other: CountStat): CountStat = {
    val plus = new CountStat(ecql)
    plus.count = this.count + other.count
    plus
  }

  override def +=(other: CountStat): Unit = count += other.count

  override def toJson(): String = s"""{ "count": $count }"""

  override def isEmpty: Boolean = count == 0

  override def clear(): Unit = count = 0
}