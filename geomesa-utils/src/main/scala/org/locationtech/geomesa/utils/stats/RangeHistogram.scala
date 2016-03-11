/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.reflect.ClassTag
import scala.util.parsing.json.JSONArray

/**
 * The range histogram's state is stored in an indexed array, where the index is the bin number
 * and the values are the counts.
 *
 * e.g. a range of 0 to 3 with 3 bins will result in these bins: [0, 1), [1, 2), [2, 3) and the
 * array will contain three entries.
 *
 * @param attrIndex attribute index for the attribute the histogram is being made for
 * @param numBins number of bins the histogram has
 * @param endpoints lower/upper end of histogram
 * @tparam T a comparable type which must have a StatHelperFunctions type class
 */
class RangeHistogram[T](val attrIndex: Int,
                        val attrType: String,
                        val numBins: Int,
                        val endpoints: (T, T))(implicit c: ClassTag[T]) extends Stat {

  override type S = RangeHistogram[T]

  val bins = BinnedArray[T](numBins, endpoints)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attrIndex)
    if (value != null) {
      bins.add(value.asInstanceOf[T])
    }
  }

  override def +=(other: RangeHistogram[T]): RangeHistogram[T] = {
    bins.add(other.bins.counts); this
  }

  override def toJson(): String = new JSONArray(bins.counts.toList).toString()

  override def clear(): Unit = bins.clear()

  override def equals(obj: Any): Boolean = {
    obj match {
      case rh: RangeHistogram[T] =>
        attrIndex == rh.attrIndex &&
            attrType == rh.attrType &&
            numBins == rh.numBins &&
            endpoints == rh.endpoints &&
            bins == rh.bins
      case _ => false
    }
  }
}
