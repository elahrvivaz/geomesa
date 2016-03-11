/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.stats

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.Combiner
import org.joda.time.{DateTimeZone, Interval}
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata
import org.locationtech.geomesa.accumulo.iterators.IteratorClassLoader

import scala.collection.JavaConversions._

class StatsCombiner extends Combiner with LazyLogging {

  IteratorClassLoader.initClassLoader(classOf[StatsCombiner])

  override def reduce(key: Key, iter: java.util.Iterator[Value]): Value = {
    val head = iter.next()

    if (!iter.hasNext) {
      return head
    }

    val cf = key.getColumnFamily.toString
    if (cf == GeoMesaMetadata.SPATIAL_BOUNDS_KEY) {
      reduceSpatialBounds(head, iter)
    } else if (cf == GeoMesaMetadata.TEMPORAL_BOUNDS_KEY) {
      reduceTemporalBounds(head, iter)
    } else {
      logger.error(s"Found multiple metadata values when only expecting one for key $key\n" +
          s"Will keep first value ($head) and drop ${iter.mkString(", ")}")
      head
    }
  }

  private def reduceSpatialBounds(head: Value, tail: Iterator[Value]): Value = {
    val combined = GeoMesaStats.decodeSpatialBounds(head.toString)
    tail.foreach(v => combined.expandToInclude(GeoMesaStats.decodeSpatialBounds(v.toString)))
    new Value(GeoMesaStats.encode(combined).getBytes("UTF-8"))
  }

  private def reduceTemporalBounds(head: Value, tail: Iterator[Value]): Value = {
    def toEndpoints(v: Value) = {
      val i = GeoMesaStats.decodeTimeBounds(v.toString)
      (i.getStartMillis, i.getEndMillis)
    }
    val (start, end) = tail.map(toEndpoints).foldLeft(toEndpoints(head)) {
      case ((s1, e1), (s2, e2)) => (if (s1 < s2) s1 else s2, if (e1 > e2) e1 else e2)
    }
    new Value(GeoMesaStats.encode(new Interval(start, end, DateTimeZone.UTC)).getBytes("UTF-8"))
  }
}
