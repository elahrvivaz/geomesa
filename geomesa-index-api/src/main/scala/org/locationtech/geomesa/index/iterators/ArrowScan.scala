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
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DictionaryBuildingWriter, SimpleFeatureArrowFileWriter, SimpleFeatureArrowIO}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.arrow.{ArrowEncodedSft, ArrowProperties}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, QueryPlan}
import org.locationtech.geomesa.index.iterators.ArrowScan._
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureOrdering}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat, TopK}
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait ArrowScan extends AggregatingScan[ArrowAggregate] {

  private var batchSize: Int = _

  // TODO file metadata created in the iterator has an empty sft name

  override def initResult(sft: SimpleFeatureType,
                          transform: Option[SimpleFeatureType],
                          options: Map[String, String]): ArrowAggregate = {
    import AggregatingScan.Configuration.{SftOpt, TransformSchemaOpt}
    import ArrowScan.Configuration._
    import ArrowScan.aggregateCache

    batchSize = options(BatchSizeKey).toInt

    val typ = options(TypeKey)
    val (arrowSft, arrowSftString) = transform match {
      case Some(tsft) => (tsft, options(TransformSchemaOpt))
      case None       => (sft, options(SftOpt))
    }
    val includeFids = options(IncludeFidsKey)
    val dictionary = options(DictionaryKey)
    val sort = options.get(SortKey).map(name => (name, options.get(SortReverseKey).exists(_.toBoolean)))

    val cacheKey = typ + arrowSftString + includeFids + dictionary + sort

    def create(): ArrowAggregate = {
      val encoding = SimpleFeatureEncoding.min(includeFids.toBoolean)
      if (typ == Types.SinglePassType) {
        val dictionaries = dictionary.split(",").filter(_.length > 0)
        new SinglePassAggregate(arrowSft, dictionaries, encoding, sort)
      } else if (typ == Types.BatchType) {
        val dictionaries = ArrowScan.decodeDictionaries(arrowSft, dictionary)
        sort match {
          case None => new BatchAggregate(arrowSft, dictionaries, encoding)
          case Some((s, r)) => new SortingBatchAggregate(arrowSft, dictionaries, encoding, s, r)
        }
      } else if (typ == Types.FileType) {
        val dictionaries = dictionary.split(",").filter(_.length > 0)
        sort match {
          case None => new MultiFileAggregate(arrowSft, dictionaries, encoding)
          case Some((s, r)) => new MultiFileSortingAggregate(arrowSft, dictionaries, encoding, s, r)
        }
      } else {
        throw new RuntimeException(s"Expected type, got $typ")
      }
    }

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
    val IncludeFidsKey = "fids"
    val DictionaryKey  = "dict"
    val TypeKey        = "type"
    val BatchSizeKey   = "batch"
    val SortKey        = "sort"
    val SortReverseKey = "sort-rev"

    object Types {
      val BatchType      = "batch"
      val FileType       = "file"
      val SinglePassType = "single"
    }
  }

  val DictionaryTopK = SystemProperty("geomesa.arrow.dictionary.top", "1000")

  val DictionaryOrdering: Ordering[AnyRef] = new Ordering[AnyRef] {
    override def compare(x: AnyRef, y: AnyRef): Int =
      SimpleFeatureOrdering.nullCompare(x.asInstanceOf[Comparable[Any]], y)
  }

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowAggregate]

  /**
    * Configure the iterator
    *
    * @param sft simple feature type
    * @param index feature index
    * @param stats stats, used for querying dictionaries
    * @param filter full filter from the query, if any
    * @param ecql secondary push down filter, if any
    * @param hints query hints
    * @return
    */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                stats: GeoMesaStats,
                filter: Option[Filter],
                ecql: Option[Filter],
                hints: Hints): ArrowScanConfig = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._

    val arrowSft = hints.getTransformSchema.getOrElse(sft)
    val includeFids = hints.isArrowIncludeFid
    val sort = hints.getArrowSort
    val batchSize = getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(includeFids)

    val baseConfig = {
      val base = AggregatingScan.configure(sft, index, ecql, hints.getTransform, hints.getSampling)
      base ++ AggregatingScan.optionalMap(
        IncludeFidsKey -> includeFids.toString,
        SortKey        -> sort.map(_._1),
        SortReverseKey -> sort.map(_._2.toString),
        BatchSizeKey   -> batchSize.toString
      )
    }

    val dictionaryFields = hints.getArrowDictionaryFields
    val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
    val cachedDictionaries: Map[String, TopK[AnyRef]] = if (!hints.isArrowCachedDictionaries) { Map.empty } else {
      def name(i: Int): String = sft.getDescriptor(i).getLocalName
      val toLookup = dictionaryFields.filterNot(providedDictionaries.contains)
      stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => name(k.attribute) -> k).toMap
    }

    if (hints.isArrowDoublePass ||
          dictionaryFields.forall(f => providedDictionaries.contains(f) || cachedDictionaries.contains(f))) {
      // we have all the dictionary values, or we will run a query to determine them up front
      val dictionaries = createDictionaries(stats, sft, filter, dictionaryFields, providedDictionaries, cachedDictionaries)
      val config = baseConfig ++ Map(
        DictionaryKey -> encodeDictionaries(dictionaries),
        TypeKey       -> Configuration.Types.BatchType
      )
      val reduce = mergeBatches(arrowSft, dictionaries, encoding, batchSize, sort)
      ArrowScanConfig(config, reduce)
    } else if (hints.isArrowMultiFile) {
      val config = baseConfig ++ Map(
        DictionaryKey -> dictionaryFields.mkString(","),
        TypeKey       -> Configuration.Types.FileType
      )
      val reduce = mergeFiles(arrowSft, dictionaryFields, encoding, sort)
      ArrowScanConfig(config, reduce)
    } else {
      val config = baseConfig ++ Map(
        DictionaryKey -> dictionaryFields.mkString(","),
        TypeKey       -> Configuration.Types.SinglePassType
      )
      val reduce = mergeBatchesAndDictionaries(arrowSft, dictionaryFields, encoding, batchSize, sort)
      ArrowScanConfig(config, reduce)
    }
  }

  def getBatchSize(hints: Hints): Int = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)

  /**
    * Reduce function for whole arrow files coming back from the aggregating scan. Each feature
    * will have a single arrow file
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding simple feature encoding
    * @param sort sort
    * @return
    */
  def mergeFiles(sft: SimpleFeatureType,
                 dictionaryFields: Seq[String],
                 encoding: SimpleFeatureEncoding,
                 sort: Option[(String, Boolean)]): QueryPlan.Reducer = {
    // we don't need to manipulate anything, just return the file batches from the distributed scan
    (iter) => {
      // ensure that we return something
      if (iter.hasNext) { iter } else {
        // iterator is empty but this will pass it through to be closed
        val bytes = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
        val dictionaries = createEmptyDictionaries(dictionaryFields)
        val result = SimpleFeatureArrowIO.createFile(sft, dictionaries, encoding, sort)(bytes)
        val sf = resultFeature()
        result.map { bytes => sf.setAttribute(0, bytes); sf }
      }
    }
  }

  /**
    * Reduce function for batches with a common schema.
    *
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches, final feature contains EOF indicator
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding
    * @param batchSize batch size
    * @param sort sort
    * @return
    */
  def mergeBatches(sft: SimpleFeatureType,
                   dictionaries: Map[String, ArrowDictionary],
                   encoding: SimpleFeatureEncoding,
                   batchSize: Int,
                   sort: Option[(String, Boolean)]): QueryPlan.Reducer = {
    def sorted(iter: CloseableIterator[SimpleFeature]): CloseableIterator[Array[Byte]] = {
      val bytes = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
      if (sort.isEmpty) { bytes } else {
        val (attribute, reverse) = sort.get
        SimpleFeatureArrowIO.sortBatches(sft, dictionaries, encoding, attribute, reverse, batchSize, bytes)
      }
    }
    (iter) => {
      val result = SimpleFeatureArrowIO.createFile(sft, dictionaries, encoding, sort)(sorted(iter))
      val sf = resultFeature()
      result.map { bytes => sf.setAttribute(0, bytes); sf }
    }
  }

  /**
    * Reduce function for batches with disparate dictionaries.
    *
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches, final feature contains EOF indicator
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries
    * @param encoding encoding
    * @param batchSize batch size
    * @param sort sort
    * @return
    */
  def mergeBatchesAndDictionaries(sft: SimpleFeatureType,
                                  dictionaryFields: Seq[String],
                                  encoding: SimpleFeatureEncoding,
                                  batchSize: Int,
                                  sort: Option[(String, Boolean)]): QueryPlan.Reducer = {
    // merge the files coming back into a single file with batches
    (iter) => {
      // note: get bytes before expanding to array as the simple feature may be re-used
      val files = try { iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]]).toArray } finally { iter.close() }
      val result = if (files.isEmpty) {
        // ensure that we return something
        val dictionaries = createEmptyDictionaries(dictionaryFields)
        SimpleFeatureArrowIO.createFile(sft, dictionaries, encoding, sort)(CloseableIterator.empty)
      } else if (files.length == 1) {
        // if only a single batch, we can just return it
        CloseableIterator(files.iterator)
      } else {
        SimpleFeatureArrowIO.mergeFiles(sft, files, dictionaryFields, encoding, sort, batchSize)
      }
      val sf = resultFeature()
      result.map { bytes => sf.setAttribute(0, bytes); sf }
    }
  }

  /**
    * Determine dictionary values, as required. Priority:
    *   1. values provided by the user
    *   2. cached topk stats
    *   3. enumeration stats query against result set
    *
    * @param stats stats
    * @param sft simple feature type
    * @param filter full filter for the query being run, used if querying enumeration values
    * @param attributes names of attributes to dictionary encode
    * @param provided provided dictionary values, if any, keyed by attribute name
    * @return
    */
  def createDictionaries(stats: GeoMesaStats,
                         sft: SimpleFeatureType,
                         filter: Option[Filter],
                         attributes: Seq[String],
                         provided: Map[String, Array[AnyRef]],
                         cached: Map[String, TopK[AnyRef]]): Map[String, ArrowDictionary] = {
    def sort(values: Array[AnyRef]): Unit = java.util.Arrays.sort(values, DictionaryOrdering)

    if (attributes.isEmpty) { Map.empty } else {
      var id = -1L
      // note: sort values to return same dictionary cache
      val providedDictionaries = provided.map { case (k, v) => id += 1; sort(v); k -> ArrowDictionary.create(id, v) }
      val toLookup = attributes.filterNot(provided.contains)
      if (toLookup.isEmpty) { providedDictionaries } else {
        // use topk if available, otherwise run a live stats query to get the dictionary values
        val queried = if (toLookup.forall(cached.contains)) {
          cached.map { case (name, k) =>
            id += 1
            val values = k.topK(DictionaryTopK.get.toInt).map(_._1).toArray
            sort(values)
            name -> ArrowDictionary.create(id, values)
          }
        } else {
          // if we have to run a query, might as well generate all values
          val query = Stat.SeqStat(toLookup.map(Stat.Enumeration))
          val enumerations = stats.runStats[EnumerationStat[String]](sft, query, filter.getOrElse(Filter.INCLUDE))
          // enumerations should come back in the same order
          // this has been fixed, but previously we couldn't use the enumeration attribute
          // number b/c it might reflect a transform sft
          val nameIter = toLookup.iterator
          enumerations.map { e =>
            id += 1
            val values = e.values.toArray[AnyRef]
            sort(values)
            nameIter.next -> ArrowDictionary.create(id, values)
          }.toMap
        }
        queried ++ providedDictionaries
      }
    }
  }

  private def createEmptyDictionaries(fields: Seq[String]): Map[String, ArrowDictionary] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike
    fields.mapWithIndex { case (name, i) => name -> ArrowDictionary.create(i, Array.empty) }.toMap
  }

  def resultFeature(): SimpleFeature =
    new ScalaSimpleFeature(ArrowEncodedSft, "", Array(null, GeometryUtils.zeroPoint))

  /**
    * Encodes the dictionaries as a string for passing to the iterator config
    *
    * @param dictionaries dictionaries
    * @return
    */
  private def encodeDictionaries(dictionaries: Map[String, ArrowDictionary]): String =
    StringSerialization.encodeSeqMap(dictionaries.map { case (k, v) => k -> v.iterator.toSeq })

  // TODO optimize these

  /**
    * Decodes an encoded dictionary string from an iterator config
    *
    * @param encoded dictionary string
    * @return
    */
  private def decodeDictionaries(sft: SimpleFeatureType, encoded: String): Map[String, ArrowDictionary] = {
    var id = -1L
    StringSerialization.decodeSeqMap(sft, encoded).map { case (k, v) =>
      id += 1
      k -> ArrowDictionary.create(id, v)
    }
  }

  /**
    * Trait for aggregating arrow files
    */
  trait ArrowAggregate {
    def size: Int
    def add(sf: SimpleFeature): Unit
    def encode(): Array[Byte]
    def clear(): Unit
    def withBatchSize(size: Int): ArrowAggregate

    def isEmpty: Boolean = size == 0
  }

  /**
    * Returns full arrow files, with metadata. Builds dictionaries on the fly. Doesn't sort
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries fields
    * @param encoding encoding
    */
  class MultiFileAggregate(sft: SimpleFeatureType, dictionaryFields: Seq[String], encoding: SimpleFeatureEncoding)
      extends ArrowAggregate {

    private val writer = DictionaryBuildingWriter.create(sft, dictionaryFields, encoding)
    private val os = new ByteArrayOutputStream()

    override def add(sf: SimpleFeature): Unit = writer.add(sf)

    override def size: Int = writer.size

    override def clear(): Unit = writer.clear()

    override def encode(): Array[Byte] = {
      os.reset()
      writer.encode(os)
      os.toByteArray
    }

    override def withBatchSize(size: Int): ArrowAggregate = this
  }

  class MultiFileSortingAggregate(sft: SimpleFeatureType,
                                  dictionaryFields: Seq[String],
                                  encoding: SimpleFeatureEncoding,
                                  sortField: String,
                                  reverse: Boolean) extends ArrowAggregate {

    private var index = 0
    private var features: Array[SimpleFeature] = _

    private val writer = DictionaryBuildingWriter.create(sft, dictionaryFields, encoding)
    private val os = new ByteArrayOutputStream()

    private val ordering = {
      val o = SimpleFeatureOrdering(sft.indexOf(sortField))
      if (reverse) { o.reverse } else { o }
    }

    override def add(sf: SimpleFeature): Unit = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = {
      index = 0
      writer.clear()
    }

    override def encode(): Array[Byte] = {
      java.util.Arrays.sort(features, 0, index, ordering)

      var i = 0
      while (i < index) {
        writer.add(features(i))
        i += 1
      }

      os.reset()
      writer.encode(os)
      os.toByteArray
    }

    override def withBatchSize(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim[SimpleFeature](size)
      }
      this
    }
  }

  /**
    * Returns record batches without any metadata. Dictionaries must be known up front. Doesn't sort
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding
    */
  class BatchAggregate(sft: SimpleFeatureType,
                       dictionaries: Map[String, ArrowDictionary],
                       encoding: SimpleFeatureEncoding) extends ArrowAggregate {

    private var index = 0

    private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    private val batchWriter = new RecordBatchUnloader(vector)

    override def add(sf: SimpleFeature): Unit = {
      vector.writer.set(index, sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = {
      vector.clear()
      index = 0
    }

    override def encode(): Array[Byte] = batchWriter.unload(index)

    override def withBatchSize(size: Int): ArrowAggregate = this
  }

  /**
    * Returns record batches without any metadata. Dictionaries must be known up front. Sorts
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding
    * @param sortField sort field
    * @param reverse sort reverse
    */
  class SortingBatchAggregate(sft: SimpleFeatureType,
                              dictionaries: Map[String, ArrowDictionary],
                              encoding: SimpleFeatureEncoding,
                              sortField: String,
                              reverse: Boolean) extends ArrowAggregate {

    private var index = 0
    private var features: Array[SimpleFeature] = _

    private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    private val batchWriter = new RecordBatchUnloader(vector)

    private val ordering = {
      val o = SimpleFeatureOrdering(sft.indexOf(sortField))
      if (reverse) { o.reverse } else { o }
    }

    override def add(sf: SimpleFeature): Unit = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = {
      index = 0
      vector.clear()
    }

    override def encode(): Array[Byte] = {
      java.util.Arrays.sort(features, 0, index, ordering)

      var i = 0
      while (i < index) {
        vector.writer.set(i, features(i))
        i += 1
      }
      batchWriter.unload(index)
    }

    override def withBatchSize(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim[SimpleFeature](size)
      }
      this
    }
  }

  /**
    * Arrow aggregate
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding arrow encoding
    * @param sort sort field, sort reverse
    */
  class SinglePassAggregate(sft: SimpleFeatureType,
                            dictionaryFields: Seq[String],
                            encoding: SimpleFeatureEncoding,
                            sort: Option[(String, Boolean)]) extends ArrowAggregate {

    private var index = 0
    private var features: Array[SimpleFeature] = _
    private var dictionaryValues = scala.collection.mutable.Map.empty[String, Array[AnyRef]]

    private val os = new ByteArrayOutputStream()

    private val ordering = sort.map { case (name, reverse) =>
      val o = SimpleFeatureOrdering(sft.indexOf(name))
      if (reverse) { o.reverse } else { o }
    }

    override def add(sf: SimpleFeature): Unit = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
    }

    override def size: Int = index

    override def clear(): Unit = {
      index = 0
      os.reset()
    }

    override def encode(): Array[Byte] = {
      import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

      ordering.foreach(o => java.util.Arrays.sort(features, 0, index, o))

      val dictionaries = dictionaryFields.mapWithIndex { case (name, dictionaryId) =>
        val values = dictionaryValues(name)
        val attribute = sft.indexOf(name)
        val seen = scala.collection.mutable.HashSet.empty[AnyRef]
        var count = 0
        var i = 0
        while (i < index) {
          val value = features(i).getAttribute(attribute)
          if (seen.add(value)) {
            values(count) = value
            count += 1
          }
          i += 1
        }
        // note: we sort the dictionary values to make them easier to merge later
        java.util.Arrays.sort(values, 0, count, DictionaryOrdering)
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

    override def withBatchSize(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim(size)
        dictionaryFields.foreach { field => dictionaryValues(field) = Array.ofDim(size) }
      }
      this
    }
  }

  case class ArrowScanConfig(config: Map[String, String], reduce: QueryPlan.Reducer)
}