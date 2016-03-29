/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.commons.codec.binary.Base64
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.buildTypeName
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Reads simple features and observe them with a Stat server-side
 *
 * Only works with z3IdxStrategy for now (queries that date filters)
 */
class KryoLazyStatsIterator extends KryoLazyAggregatingIterator[Stat] {

  import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator._

  var serializer: KryoFeatureSerializer = null
  var featureToSerialize: SimpleFeature = null

  override def init(options: Map[String, String]): Stat = {
    val statString = options(STATS_STRING_KEY)
    sft = SimpleFeatureTypes.createType("", options(KryoLazyAggregatingIterator.SFT_OPT))

    val statsSft = SimpleFeatureTypes.createType("", STATS_ITERATOR_SFT_STRING)
    serializer = new KryoFeatureSerializer(statsSft)
    featureToSerialize = new ScalaSimpleFeature("", statsSft, Array(null, GeometryUtils.zeroPoint))

    Stat(sft, statString)
  }

  override def aggregateResult(sf: SimpleFeature, result: Stat): Unit = result.observe(sf)

  override def encodeResult(result: Stat): Array[Byte] = {
    featureToSerialize.setAttribute(0, encodeStat(result, sft))
    serializer.serialize(featureToSerialize)
  }
}

object KryoLazyStatsIterator extends LazyLogging {

  val DEFAULT_PRIORITY = 30
  val STATS_STRING_KEY = "geomesa.stats.string"
  val STATS_FEATURE_TYPE_KEY = "geomesa.stats.featuretype"
  val STATS_ITERATOR_SFT_STRING = "stats:String,geom:Geometry"

  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val is = new IteratorSetting(priority, "stats-iter", classOf[KryoLazyStatsIterator])
    KryoLazyAggregatingIterator.configure(is, sft, filter, deduplicate, None)
    is.addOption(STATS_STRING_KEY, hints.get(STATS_KEY).asInstanceOf[String])
    is
  }

  def createFeatureType(origFeatureType: SimpleFeatureType) = {
    //Need a filler namespace, else geoserver throws nullptr exception for xml output
    val (namespace, name) = buildTypeName(origFeatureType.getTypeName)
    val outNamespace = if (namespace == null){
      "NullNamespace"
    } else {
      namespace
    }
    SimpleFeatureTypes.createType(outNamespace, name, KryoLazyStatsIterator.STATS_ITERATOR_SFT_STRING)
  }

  def encodeStat(stat: Stat, sft: SimpleFeatureType): String =
    Base64.encodeBase64URLSafeString(StatSerialization.pack(stat, sft))

  def decodeStat(encoded: String, sft: SimpleFeatureType): Stat =
    StatSerialization.unpack(Base64.decodeBase64(encoded), sft)

  /**
   * Reduces computed simple features which contain stat information into one on the client
   *
   * @param features iterator of features received per tablet server from query
   * @param query query that the stats are being run against
   * @return aggregated iterator of features
   */
  def reduceFeatures(features: SFIter, query: Query, sft: SimpleFeatureType): SFIter = {
    val encode = query.getHints.containsKey(RETURN_ENCODED_KEY)
    val returnSft = query.getHints.getReturnSft

    val decodedStats = features.map(f => decodeStat(f.getAttribute(0).toString, sft))

    val sum = if (decodedStats.isEmpty) {
      // get empty stats
      Stat(sft, query.getHints.get(STATS_KEY).asInstanceOf[String])
    } else {
      val sum = decodedStats.next()
      decodedStats.foreach(sum += _)
      sum
    }

    val stats = if (encode) encodeStat(sum, sft) else sum.toJson()
    Iterator(new ScalaSimpleFeature("stat", returnSft, Array(stats, GeometryUtils.zeroPoint)))
  }
}