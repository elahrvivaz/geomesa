/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data.stats

import com.vividsolutions.jts.geom.Envelope
import org.apache.accumulo.core.client.impl.{Tables, MasterClient}
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.security.thrift.TCredentials
import org.apache.accumulo.trace.instrument.Tracer
import org.geotools.data.Query
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTimeZone, Interval}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.RecordTable
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, wholeWorldEnvelope}
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._

/**
 * Tracks stats for a schema - spatial/temporal bounds, number of records, etc. Persistence of
 * stats is not part of this trait, as different implementations will likely have different method signatures.
 */
trait GeoMesaStats {

  /**
   * Gets the bounds for data that will be returned for a query
   *
   * @param query query
   * @param exact rough estimate, or precise bounds. note: precise bounds will likely be very expensive.
   * @return bounds
   */
  def getBounds(query: Query, exact: Boolean = false): ReferencedEnvelope

  /**
   * Gets the temporal bounds for data that will be returned for a query
   *
   * @param query query
   * @param exact rough estimate, or precise bounds. note: precise bounds will likely be very expensive.
   * @return time bounds
   */
  def getTemporalBounds(query: Query, exact: Boolean = false): Interval

  /**
   * Gets the number of features that will be returned for a query
   *
   * @param query query
   * @param exact rough estimate, or precise count. note: precise count will likely be very expensive.
   * @return count of features
   */
  def getCount(query: Query, exact: Boolean = false): Long
}

/**
 * Tracks stats via entries stored in metadata
 */
trait GeoMesaMetadataStats extends GeoMesaStats {

  this: AccumuloDataStore =>

  import GeoMesaStats.{allTimeBounds, decodeSpatialBounds, decodeTimeBounds, encode}

  /**
   * Get rough bounds for a query
   * Note: we don't currently filter by the cql
   *
   * @param query query
   * @return bounds
   */
  override def getBounds(query: Query, exact: Boolean = false): ReferencedEnvelope =
    readSpatialBounds(query.getTypeName)
        .map(new ReferencedEnvelope(_, CRS_EPSG_4326))
        .getOrElse(wholeWorldEnvelope)

  /**
   * Get rough time bounds for a query
   * Note: we don't currently filter by the cql
   *
   * @param query query
   * @return time bounds
   */
  override def getTemporalBounds(query: Query, exact: Boolean = false): Interval =
    readTemporalBounds(query.getTypeName).getOrElse(allTimeBounds)

  /**
   * Rough estimate of number of features
   * Note: we don't currently filter by the cql
   *
   * @param query query
   * @return count of features
   */
  override def getCount(query: Query, exact: Boolean = false): Long =
    retrieveTableSize(getTableName(query.getTypeName, RecordTable))

  /**
   * Writes spatial bounds for this feature
   *
   * @param typeName simple feature type
   * @param bounds partial bounds - existing bounds will be expanded to include this
   */
  def writeSpatialBounds(typeName: String, bounds: Envelope): Unit = {
    val toWrite = readSpatialBounds(typeName) match {
      case Some(current) if current == bounds => None
      case _                                  => Some(bounds)
    }
    toWrite.foreach(b => metadata.insert(typeName, SPATIAL_BOUNDS_KEY, encode(b)))
  }

  /**
   * Writes temporal bounds for this feature
   *
   * @param typeName simple feature type
   * @param bounds partial bounds - existing bounds will be expanded to include this
   */
  def writeTemporalBounds(typeName: String, bounds: Interval): Unit = {
    val toWrite = readTemporalBounds(typeName) match {
      case Some(current) if current == bounds => None
      case _                                  => Some(bounds)
    }
    toWrite.foreach(b => metadata.insert(typeName, TEMPORAL_BOUNDS_KEY, encode(b)))
  }

  private def readTemporalBounds(typeName: String): Option[Interval] =
    metadata.read(typeName, TEMPORAL_BOUNDS_KEY, cache = false).filterNot(_.isEmpty).map(decodeTimeBounds)

  private def readSpatialBounds(typeName: String): Option[Envelope] =
    metadata.read(typeName, SPATIAL_BOUNDS_KEY, cache = false).filterNot(_.isEmpty).map(decodeSpatialBounds)

  // This lazily computed function helps shortcut getCount from scanning entire tables.
  private lazy val retrieveTableSize: (String) => Long =
    if (connector.isInstanceOf[MockConnector]) {
      (tableName: String) => -1
    } else {
      val masterClient = MasterClient.getConnection(connector.getInstance())
      val tc = new TCredentials()
      // TODO this will get stale, no?
      val mmi = masterClient.getMasterStats(Tracer.traceInfo(), tc)
      (tableName: String) => {
        val tableId = Tables.getTableId(connector.getInstance(), tableName)
        val v = mmi.getTableMap.get(tableId)
        v.getRecs
      }
    }
}

object GeoMesaStats {

  def allTimeBounds = new Interval(0L, System.currentTimeMillis(), DateTimeZone.UTC) // Epoch till now

  def decodeTimeBounds(value: String): Interval = {
    val longs = value.split(":").map(java.lang.Long.parseLong)
    require(longs(0) <= longs(1))
    require(longs.length == 2)
    new Interval(longs(0), longs(1), DateTimeZone.UTC)
  }

  def decodeSpatialBounds(string: String): Envelope = {
    val minMaxXY = string.split(":")
    require(minMaxXY.size == 4)
    new Envelope(minMaxXY(0).toDouble, minMaxXY(1).toDouble, minMaxXY(2).toDouble, minMaxXY(3).toDouble)
  }

  def encode(bounds: Interval): String = s"${bounds.getStartMillis}:${bounds.getEndMillis}"

  def encode(bounds: Envelope): String =
    Seq(bounds.getMinX, bounds.getMaxX, bounds.getMinY, bounds.getMaxY).mkString(":")
}
