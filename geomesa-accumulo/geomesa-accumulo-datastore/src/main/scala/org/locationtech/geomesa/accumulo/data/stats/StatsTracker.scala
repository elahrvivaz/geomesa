/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.stats

import java.io.{Closeable, Flushable}
import java.util.Date

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.joda.time.{DateTimeZone, Interval}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Trait for tracking stats based on simple features
 */
trait StatsTracker extends Flushable with Closeable {
  def visit(sf: SimpleFeature): Unit
}

object StatsTracker {
  def apply(stats: GeoMesaStats, sft: SimpleFeatureType): StatsTracker = {
    stats match {
      case s: GeoMesaMetadataStats => new MetadataStatsTracker(s, sft)
      case _ => throw new NotImplementedError("Stats implementation not supported")
    }
  }
}

/**
 * Stores stats as metadata entries
 *
 * @param stats persistence
 * @param sft simple feature type
 */
class MetadataStatsTracker(stats: GeoMesaMetadataStats, sft: SimpleFeatureType) extends StatsTracker {

  private var bounds: Envelope = null
  private var interval: Interval = null

  private val geomIndex = Option(sft.getGeomIndex).filter(_ != -1)
  private val dtgIndex = sft.getDtgIndex

  private val setStats: (SimpleFeature) => Unit = (geomIndex, dtgIndex) match {
    case (Some(geom), Some(dtg)) => (sf) => { setSpatialBounds(sf, geom); setTemporalBounds(sf, dtg) }
    case (Some(geom), None)      => (sf) => { setSpatialBounds(sf, geom) }
    case (None, Some(dtg))       => (sf) => { setTemporalBounds(sf, dtg) }
    case (None, None)            => (_) => Unit
  }

  override def visit(sf: SimpleFeature): Unit = setStats(sf)

  override def close(): Unit = flush()

  override def flush(): Unit = {
    if (bounds != null) {
      stats.writeSpatialBounds(sft.getTypeName, bounds)
      bounds = null
    }
    if (interval != null) {
      stats.writeTemporalBounds(sft.getTypeName, interval)
      interval = null
    }
  }

  private def setSpatialBounds(sf: SimpleFeature, geom: Int): Unit = {
    val env = sf.getAttribute(geom).asInstanceOf[Geometry].getEnvelopeInternal
    if (bounds == null) {
      bounds = env
    } else {
      bounds.expandToInclude(env)
    }
  }

  private def setTemporalBounds(sf: SimpleFeature, dtg: Int): Unit = {
    val date = sf.getAttribute(dtg).asInstanceOf[Date]
    if (date != null) {
      val time = date.getTime
      // note: intervals are [start, end)
      if (interval == null) {
        interval = new Interval(time, time + 1, DateTimeZone.UTC)
      } else if (interval.getStartMillis > time) {
        interval = interval.withStartMillis(time)
      } else if (interval.getEndMillis < time + 1) {
        interval = interval.withEndMillis(time + 1)
      }
    }
  }
}
