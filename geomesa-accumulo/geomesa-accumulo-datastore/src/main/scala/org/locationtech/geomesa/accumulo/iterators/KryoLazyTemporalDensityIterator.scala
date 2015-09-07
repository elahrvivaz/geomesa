/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{HashMap => JHMap, Map => jMap, UUID}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.accumulo.iterators.KryoLazyAggregatingIterator._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.buildTypeName
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes, TimeSnap}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.collection.{breakOut, mutable}
import scala.util.parsing.json.JSONObject

class KryoLazyTemporalDensityIterator extends KryoLazyAggregatingIterator[DateTime, Long] {

  import KryoLazyTemporalDensityIterator._

  var snap: TimeSnap = null
  var dtgIndex: Int = -1
  var serializer: KryoFeatureSerializer = null
  var featureToSerialize: SimpleFeature = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(src, jOptions, env)

    dtgIndex = sft.getDtgField.map(sft.indexOf).getOrElse(throw new IllegalArgumentException("dtg field required"))

    val buckets = jOptions.get(BUCKETS_KEY).toInt
    val bounds = {
      val Array(s, e) = jOptions.get(INTERVAL_KEY).split(",").map(_.toLong)
      new Interval(s, e)
    }
    snap = new TimeSnap(bounds, buckets)

    val timeSft = SimpleFeatureTypes.createType("", TEMPORAL_DENSITY_SFT_STRING)
    serializer = new KryoFeatureSerializer(timeSft)
    featureToSerialize = new ScalaSimpleFeature("", timeSft, Array(null, GeometryUtils.zeroPoint))
  }

  override def aggregateResult(sf: SimpleFeature, result: TimeSeries): Unit = {
    val date = new DateTime(sf.getAttribute(dtgIndex))
    val t = snap.t(snap.i(date))
    result.put(t, result.getOrElse(t, 0L) + 1L)
  }

  override def encodeResult(result: TimeSeries): Array[Byte] = {
    featureToSerialize.setAttribute(0, encodeTimeSeries(result))
    serializer.serialize(featureToSerialize)
  }
}

object KryoLazyTemporalDensityIterator extends Logging {

  type TimeSeries = mutable.Map[DateTime, Long]

  val TEMPORAL_DENSITY_SFT_STRING = s"timeseries:String,*geom:Point:srid=4326"
  val DEFAULT_PRIORITY = 30

  private val INTERVAL_KEY = "interval"
  private val BUCKETS_KEY = "buckets"

  val geomFactory = JTSFactoryFinder.getGeometryFactory
  private val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                hints: Hints,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val interval = hints.get(TIME_INTERVAL_KEY).asInstanceOf[Interval]
    val buckets = hints.get(TIME_BUCKETS_KEY).asInstanceOf[Int]

    val is = new IteratorSetting(priority, "temporal-density-iter", classOf[KryoLazyTemporalDensityIterator])
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    is.addOption(INTERVAL_KEY,  s"${interval.getStart.getMillis},${interval.getEnd.getMillis}")
    is.addOption(BUCKETS_KEY, s"$buckets")
    is
  }

  def createFeatureType(baseType: SimpleFeatureType) = {
    // Need a filler namespace, else geoserver throws nullptr exception for xml output
    val (namespace, name) = buildTypeName(baseType.getTypeName)
    val outNamespace = if (namespace == null) "NullNamespace" else namespace
    SimpleFeatureTypes.createType(outNamespace, name, TEMPORAL_DENSITY_SFT_STRING)
  }

  def timeSeriesToJSON(ts : TimeSeries): String = {
    val jsonMap = ts.toMap.map { case (k, v) => k.toString(df) -> v }
    new JSONObject(jsonMap).toString()
  }

  def jsonToTimeSeries(ts : String): TimeSeries = {
    val objMapper: ObjectMapper = new ObjectMapper()
    val stringMap: JHMap[String, Long] = objMapper.readValue(ts, new TypeReference[JHMap[String, java.lang.Long]]() {})
    (for((k,v) <- stringMap) yield df.parseDateTime(k) -> v)(breakOut)
  }

  def encodeTimeSeries(timeSeries: TimeSeries): String = {
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    for((date,count) <- timeSeries) {
      os.writeLong(date.getMillis)
      os.writeLong(count)
    }
    os.flush()
    Base64.encodeBase64URLSafeString(baos.toByteArray)
  }

  def decodeTimeSeries(encoded: String): TimeSeries = {
    val bytes = Base64.decodeBase64(encoded)
    val is = new DataInputStream(new ByteArrayInputStream(bytes))
    val table = new collection.mutable.HashMap[DateTime, Long]()
    while(is.available() > 0) {
      val dateIdx = new DateTime(is.readLong(), DateTimeZone.UTC)
      val weight = is.readLong()
      table.put(dateIdx, weight)
    }
    table
  }

  def combineTimeSeries(ts1: TimeSeries, ts2: TimeSeries) : TimeSeries = {
    val resultTS = new collection.mutable.HashMap[DateTime, Long]()
    for (key <- ts1.keySet ++ ts2.keySet) {
      resultTS.put(key, ts1.getOrElse(key, 0L) + ts2.getOrElse(key,0L))
    }
    resultTS
  }

  def reduceTemporalFeatures(features: SFIter, query: Query): SFIter = {
    val encode = query.getHints.containsKey(RETURN_ENCODED)
    val sft = query.getHints.getReturnSft

    val timeSeriesStrings = features.map(f => decodeTimeSeries(f.getAttribute(0).asInstanceOf[String]))
    val summedTimeSeries = timeSeriesStrings.reduceOption(combineTimeSeries)

    summedTimeSeries.iterator.map { sum =>
      val time = if (encode) encodeTimeSeries(sum) else timeSeriesToJSON(sum)
      new ScalaSimpleFeature(UUID.randomUUID().toString, sft, Array(time, GeometryUtils.zeroPoint))
    }
  }
}
