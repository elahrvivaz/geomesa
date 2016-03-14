/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.json.JSONObject

/**
 * An EnumeratedHistogram is merely a HashMap mapping values to number of occurrences
 *
 * @param attribute attribute index for the attribute the histogram is being made for
 * @tparam T some type T (which is restricted by the stat parser upstream of EnumeratedHistogram instantiation)
 */
class EnumeratedHistogram[T](val attribute: Int) extends Stat {

  override type S = EnumeratedHistogram[T]

  val frequencyMap = scala.collection.mutable.HashMap.empty[T, Long].withDefaultValue(0)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      frequencyMap(value) += 1
    }
  }

  override def +=(other: EnumeratedHistogram[T]): EnumeratedHistogram[T] = {
    other.frequencyMap.foreach { case (key, count) => frequencyMap(key) += count }; this
  }

  override def toJson(): String =
    new JSONObject(frequencyMap.toMap.map { case (k, v) => k.toString -> v }).toString()

  override def clear(): Unit = frequencyMap.clear()
}
