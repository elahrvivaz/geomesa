/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Range => aRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Aggregating iterator - only works on kryo-encoded features
 */
abstract class KryoLazyAggregatingIterator[K, V] extends SortedKeyValueIterator[Key, Value] {

  import KryoLazyAggregatingIterator._

  var sft: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key, Value] = null
  private var filter: Filter = null

  // map of our snapped points to accumulated weight
  private val result = mutable.Map.empty[K, V]

  protected var topKey: Key = null
  private var topValue: Value = new Value()
  private var currentRange: aRange = null

  private var reusableSf: KryoBufferSimpleFeature = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = src.deepCopy(env)
    val options = jOptions.asScala

    sft = SimpleFeatureTypes.createType("", options(SFT_OPT))
    filter = options.get(CQL_OPT).map(FastFilterFactory.toFilter).orNull
    reusableSf = new KryoFeatureSerializer(sft).getReusableFeature
  }

  override def hasTop: Boolean = topKey != null
  override def getTopKey: Key = topKey
  override def getTopValue: Value = topValue

  override def seek(range: aRange, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit = {
    currentRange = range
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def next(): Unit = {
    if (!source.hasTop) {
      topKey = null
      topValue = null
    } else {
      findTop()
    }
  }

  def findTop(): Unit = {
    result.clear()

    while (source.hasTop && !currentRange.afterEndKey(source.getTopKey)) {
      val sf = decode(source.getTopValue.get())
      if (filter == null || filter.evaluate(sf)) {
        topKey = source.getTopKey
        aggregateResult(sf, result) // write the record to our aggregated results
      }
      source.next() // Advance the source iterator
    }

    if (result.isEmpty) {
      topKey = null // hasTop will be false
      topValue = null
    } else {
      if (topValue == null) {
        // only re-create topValue if it was nulled out
        topValue = new Value()
      }
      topValue.set(encodeResult(result))
    }
  }

  // delegate method to allow overrides in non-kryo subclasses
  def decode(value: Array[Byte]): SimpleFeature = {
    reusableSf.setBuffer(value)
    reusableSf
  }

  def aggregateResult(sf: SimpleFeature, result: mutable.Map[K, V]): Unit
  def encodeResult(result: mutable.Map[K, V]): Array[Byte]

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object KryoLazyAggregatingIterator extends Logging {

  // configuration keys
  protected[iterators] val SFT_OPT = "sft"
  protected[iterators] val CQL_OPT = "cql"
}