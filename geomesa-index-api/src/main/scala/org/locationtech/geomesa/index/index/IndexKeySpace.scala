/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.IndexKeySpace.{ByteRange, ScanRange, ToIndexKeyBytes}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Conversions to/from index keys, without any shards, table sharing, etc
  *
  * @param ordering implicit ordering for the key space values
  * @tparam T values extracted from a filter and used for creating ranges - extracted geometries, z-ranges, etc
  * @tparam U a single key space index value, e.g. Long for a z-value, etc
  */
abstract class IndexKeySpace[T, U](implicit val ordering: Ordering[U]) {

  /**
    * Can be used with the simple feature type or not
    *
    * @param sft simple feature type
    * @return
    */
  def supports(sft: SimpleFeatureType): Boolean

  /**
    * Length of an index key
    *
    * @return
    */
  def indexKeyByteLength: Int

  /**
    * Index key from the attributes of a simple feature
    *
    * @param sft simple feature type
    * @return
    */
  def toIndexKey(sft: SimpleFeatureType, lenient: Boolean = false): (SimpleFeature) => Seq[U]

  /**
    * Index key from the attributes of a simple feature
    *
    * @param sft simple feature type
    * @param lenient if input values should be strictly checked, or normalized instead
    * @return (sequential prefixes, simple feature, suffix) => Seq(key bytes)
    */
  def toIndexKeyBytes(sft: SimpleFeatureType, lenient: Boolean = false): ToIndexKeyBytes

  /**
    * Extracts values out of the filter used for range and push-down predicate creation
    *
    * @param sft simple feature type
    * @param filter query filter
    * @param explain explainer
    * @return
    */
  def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): T

  /**
    * Creates ranges over the index keys
    *
    * @param values index values @see getIndexValues
    * @param multiplier hint for how many times the ranges will be multiplied. can be used to
    *                   inform the number of ranges generated
    * @return
    */
  def getRanges(values: T, multiplier: Int = 1): Iterator[ScanRange[U]]

  /**
    * Creates bytes from ranges
    *
    * @param ranges typed scan ranges. @see `getRanges`
    * @param prefixes prefixes to the range, if any. each prefix will create a new range
    * @return
    */
  def getRangeBytes(ranges: Iterator[ScanRange[U]],
                    prefixes: Seq[Array[Byte]] = Seq.empty,
                    tier: Boolean = false): Iterator[ByteRange]

  /**
    * Determines if the ranges generated by `getRanges` are sufficient to fulfill the query,
    * or if additional filtering needs to be done
    *
    * @param config data store config
    * @param values index values @see getIndexValues
    * @param hints query hints
    * @return
    */
  def useFullFilter(values: Option[T],
                    config: Option[GeoMesaDataStoreConfig],
                    hints: Hints): Boolean
}

object IndexKeySpace {

  /**
    * (sequential prefixes, simple feature, suffix) => Seq(key bytes)
    *
    * Creates one or more input keys, based on pre-computed values and a dynamic feature.
    * The prefixes and suffix are passed in to save creating and then copying an extra byte array.
    * The prefixes are meant to be sequentially joined into a single prefix for each key.
    */
  type ToIndexKeyBytes = (Seq[Array[Byte]], SimpleFeature, Array[Byte]) => Seq[Array[Byte]]

  /**
    * Trait for ranges of keys that have been converted into bytes
    */
  sealed trait ByteRange

  // normal range with two endpoints
  case class BoundedByteRange(lower: Array[Byte], upper: Array[Byte]) extends ByteRange
  // special case where a range matches a single row - needs to be handled differently sometimes
  case class SingleRowByteRange(row: Array[Byte]) extends ByteRange
  // only returned from `getRangeBytes` if 'tier = true' - indicates that one or both ends of the range can't
  // be tiered with additional values
  // if both ends can be tiered, prefer to use a normal BoundedByteRange
  case class TieredByteRange(lower: Array[Byte],
                             upper: Array[Byte],
                             lowerTierable: Boolean = false,
                             upperTierable: Boolean = false) extends ByteRange

  object ByteRange {

    import ByteArrays.ByteOrdering
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    val UnboundedLowerRange: Array[Byte] = Array.empty
    val UnboundedUpperRange: Array[Byte] = Array.fill(3)(ByteArrays.MaxByte)

    def min(ranges: Seq[ByteRange]): Array[Byte] = {
      ranges.collect {
        case BoundedByteRange(lo, _) => lo
        case SingleRowByteRange(row) => row
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }.minOption.getOrElse(UnboundedLowerRange)
    }

    def max(ranges: Seq[ByteRange]): Array[Byte] = {
      ranges.collect {
        case BoundedByteRange(_, hi) => hi
        case SingleRowByteRange(row) => row
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }.maxOption.getOrElse(UnboundedUpperRange)
    }
  }

  /**
    * Ranges of native key objects, that haven't been converted to bytes yet
    *
    * @tparam T key type
    */
  sealed trait ScanRange[T]

  // specialize long to avoid boxing for z2/xz2 index
  case class BoundedRange[@specialized(Long) T](lower: T, upper: T) extends ScanRange[T]
  case class SingleRowRange[T](row: T) extends ScanRange[T]
  case class PrefixRange[T](prefix: T) extends ScanRange[T]
  case class LowerBoundedRange[T](lower: T) extends ScanRange[T]
  case class UpperBoundedRange[T](upper: T) extends ScanRange[T]
  case class UnboundedRange[T](empty: T) extends ScanRange[T]
}
