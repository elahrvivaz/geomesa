/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.stats

import java.io.{Closeable, Flushable}

import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType._
import org.locationtech.geomesa.utils.stats.RangeHistogram
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Tracks stats for a schema - spatial/temporal bounds, number of records, etc. Persistence of
 * stats is not part of this trait, as different implementations will likely have different method signatures.
 */
trait GeoMesaStats extends Closeable {

  /**
    * Gets the number of features that will be returned for a query. May return -1 if exact is false
    * and estimate is unavailable.
    *
    * @param sft simple feature type
    * @param filter cql filter
    * @param exact rough estimate, or precise count. note: precise count will likely be expensive.
    * @return count of features, or -1 if exact if false and estimate is unavailable
    */
  def getCount(sft: SimpleFeatureType, filter: Filter = Filter.INCLUDE, exact: Boolean = false): Long

  /**
    * Gets the bounds for data that will be returned for a query
    *
    * @param sft simple feature type
    * @param filter cql filter
    * @param exact rough estimate, or precise bounds. note: precise bounds will likely be expensive.
    * @return bounds
    */
  def getBounds(sft: SimpleFeatureType, filter: Filter = Filter.INCLUDE, exact: Boolean = false): ReferencedEnvelope

  /**
    * Gets the minimum and maximum values for the given attribute
    *
    * @param sft simple feature type
    * @param attribute attribute name to examine
    * @param filter cql filter
    * @param exact rough estimate, or precise values. note: precise values will likely be expensive.
    * @return mix/max values. types will be consistent with the binding of the attribute
    */
  def getMinMax[T](sft: SimpleFeatureType, attribute: String, filter: Filter = Filter.INCLUDE, exact: Boolean = false): (T, T)

  /**
    * Get a histogram of values for the given attribute
    *
    * @param sft simple feature type
    * @param attribute attribute name to examine
    * @return histogram of values. types will be consistent with the binding the attribute
    */
  def getHistogram[T](sft: SimpleFeatureType, attribute: String): Option[RangeHistogram[T]]

  /**
    * Gets an object to track stats as they are written
    *
    * @param sft simple feature type
    * @return updater
    */
  def getStatUpdater(sft: SimpleFeatureType): StatUpdater

  // use cases:
  // 1. query planning - estimate counts for different primary filters (dtg+geom, geom, attr, rec?)
  // 2. bounds visitor - estimate spatial bounds for different queries
  // 3. min/max visitors - for various attributes
}

trait HasGeoMesaStats {
  def stats: GeoMesaStats
}

/**
  * Trait for tracking stats based on simple features
  */
trait StatUpdater extends Flushable with Closeable {
  def update(sf: SimpleFeature): Unit
}
