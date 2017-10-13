/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.io.ByteArrayOutputStream

import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileWriter, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.{ArrowEncodedSft, ArrowProperties}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.ArrowScan.ArrowAggregate
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureOrdering}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait ArrowScan extends AggregatingScan[ArrowAggregate] {

  private var batchSize: Int = _

  override def initResult(sft: SimpleFeatureType,
                          transform: Option[SimpleFeatureType],
                          options: Map[String, String]): ArrowAggregate = {
    import AggregatingScan.Configuration.{SftOpt, TransformSchemaOpt}
    import ArrowScan.Configuration._
    import ArrowScan.aggregateCache

    batchSize = options(BatchSizeKey).toInt
    val encoding = SimpleFeatureEncoding.min(options(IncludeFidsKey).toBoolean)
    val (arrowSft, arrowSftString) = transform match {
      case Some(tsft) => (tsft, options(TransformSchemaOpt))
      case None       => (sft, options(SftOpt))
    }
    val dictionaries = options(DictionaryKey).split(",").filter(_.length > 0)
    val sort = options.get(SortKey).map(name => (name, options.get(SortReverseKey).exists(_.toBoolean)))

    val cacheKey = arrowSftString + encoding + dictionaries + sort

    def create() = new ArrowAggregate(arrowSft, dictionaries, encoding, sort)

    aggregateCache.getOrElseUpdate(cacheKey, create()).withBatchSize(batchSize)
  }

  override protected def notFull(result: ArrowAggregate): Boolean = result.size < batchSize

  override protected def aggregateResult(sf: SimpleFeature, result: ArrowAggregate): Unit = result.add(sf)

  override protected def encodeResult(result: ArrowAggregate): Array[Byte] = result.encode()
}

object ArrowScan {

  import org.locationtech.geomesa.arrow.allocator
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  object Configuration {
    val BatchSizeKey   = "batch"
    val DictionaryKey  = "dict"
    val IncludeFidsKey = "fids"
    val SortKey        = "sort"
    val SortReverseKey = "sort-rev"
  }

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowAggregate]

  private val ordering = new Ordering[Comparable[Any]] {
    override def compare(x: Comparable[Any], y: Comparable[Any]): Int = {
      if (x == null) {
        if (y == null) { 0 } else { -1 }
      } else if (y == null) {
        1
      } else {
        x.compareTo(y)
      }
    }
  }

  /**
    * Configure the iterator
    */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                dictionaries: Seq[String],
                hints: Hints): Map[String, String] = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._

    val base = AggregatingScan.configure(sft, index, filter, hints.getTransform, hints.getSampling)
    base ++ AggregatingScan.optionalMap(
      BatchSizeKey   -> getBatchSize(hints).toString,
      DictionaryKey  -> dictionaries.mkString(","),
      IncludeFidsKey -> hints.isArrowIncludeFid.toString,
      SortKey        -> hints.getArrowSort.map(_._1),
      SortReverseKey -> hints.getArrowSort.map(_._2.toString)
    )
  }

  /**
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches, final feature contains EOF indicator
    *
    * @param sft simple feature types
    * @param dictionaryFields dictionaries
    * @param hints query hints
    * @return
    */
  def reduceFeatures(sft: SimpleFeatureType, dictionaryFields: Seq[String], hints: Hints):
      CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {

    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid)
    val sort = hints.getArrowSort

    def emptyFile(): SimpleFeature = {
      import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike
      // construct an empty arrow file to return
      val out = new ByteArrayOutputStream
      val dictionaries = dictionaryFields.mapWithIndex { case (name, i) =>
        name -> ArrowDictionary.create(i, Array.empty)
      }.toMap
      WithClose(new SimpleFeatureArrowFileWriter(sft, out, dictionaries, encoding, sort))(_.start())
      toFeature(out.toByteArray)
    }

    if (hints.isArrowBatchFiles) {
      // we don't need to manipulate anything, just return the file batches from the distributed scan
      (iter) => {
        // ensure that we return something
        if (iter.hasNext) { iter } else { CloseableIterator(Iterator.single(emptyFile()), iter.close()) }
      }
    } else {
      // merge the files coming back into a single file with batches
      (iter) => {
        // note: get bytes before expanding to array as the simple feature may be re-used
        val files = try { iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]]).toArray } finally { iter.close() }
        if (files.isEmpty) {
          // ensure that we return something
          CloseableIterator(Iterator.single(emptyFile()), iter.close())
        } else if (files.length == 1) {
          // if only a single batch, we can just return it
          CloseableIterator(files.iterator.map(toFeature))
        } else {
          val batchSize = getBatchSize(hints)
          SimpleFeatureArrowIO.mergeFiles(sft, files, dictionaryFields, encoding, sort, batchSize).map(toFeature)
        }
      }
    }
  }

  private def getBatchSize(hints: Hints): Int =
    hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)

  private def toFeature(b: Array[Byte]): SimpleFeature =
    new ScalaSimpleFeature(ArrowEncodedSft, "", Array(b, GeometryUtils.zeroPoint))

  /**
    * Arrow aggregate
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding arrow encoding
    * @param sort sort field, sort reverse
    */
  class ArrowAggregate(sft: SimpleFeatureType,
                       dictionaryFields: Seq[String],
                       encoding: SimpleFeatureEncoding,
                       sort: Option[(String, Boolean)]) {

    private var index = 0
    private var features: Array[SimpleFeature] = _

    private val os = new ByteArrayOutputStream()

    private val ordering = sort.map { case (name, reverse) =>
      val o = SimpleFeatureOrdering(sft.indexOf(name))
      if (reverse) { o.reverse } else { o }
    }

    def withBatchSize(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim[SimpleFeature](size)
      }
      this
    }

    def add(sf: SimpleFeature): Unit = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
    }

    def isEmpty: Boolean = index == 0

    def size: Int = index

    def clear(): Unit = {
      index = 0
      os.reset()
    }

    def encode(): Array[Byte] = {
      import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

      ordering.foreach(o => java.util.Arrays.sort(features, 0, index, o))

      val dictionaries = dictionaryFields.mapWithIndex { case (name, dictionaryId) =>
        val attribute = sft.indexOf(name)
        val values = Array.ofDim[AnyRef with Comparable[Any]](index)
        val seen = scala.collection.mutable.HashSet.empty[AnyRef]
        var count = 0
        var i = 0
        while (i < index) {
          val value = features(i).getAttribute(attribute)
          if (seen.add(value)) {
            values(count) = value.asInstanceOf[AnyRef with Comparable[Any]]
            count += 1
          }
          i += 1
        }
        // note: we sort the dictionary values to make them easier to merge later
        java.util.Arrays.sort(values, 0, count, ArrowScan.ordering)
        name -> ArrowDictionary.create(dictionaryId, values, count)
      }.toMap

      WithClose(new SimpleFeatureArrowFileWriter(sft, os, dictionaries, encoding, sort)) { writer =>
        var i = 0
        while (i < index) {
          writer.add(features(i))
          i += 1
        }
      }

      os.toByteArray
    }
  }
}