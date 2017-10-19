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
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.ArrowBatchScan.Configuration._
import org.locationtech.geomesa.index.iterators.ArrowBatchScan.fileMetadata
import org.locationtech.geomesa.index.iterators.ArrowScan.{ArrowAggregate, toFeature}
import org.locationtech.geomesa.index.iterators.ArrowScan.Configuration.BatchSizeKey
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
    val sort = options.get(SortKey).map(name => (name, options.get(SortReverseKey).exists(_.toBoolean)))
    val dictionaries = options(DictionaryKey).split(",").filter(_.length > 0)

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
    val IncludeFidsKey      = "fids"
    val DictionaryFieldsKey = "dict-f"
    val DictionariesKey     = "dict"
    val MultiFileKey        = "multi"
    val SortKey             = "sort"
    val SortReverseKey      = "sort-rev"
    val BatchSizeKey        = "batch"
  }

  val DictionaryTopK = SystemProperty("geomesa.arrow.dictionary.top", "1000")

  private val aggregateCache = new SoftThreadLocalCache[String, ArrowAggregate]

  private val ordering = new Ordering[AnyRef] {
    override def compare(x: AnyRef, y: AnyRef): Int =
      SimpleFeatureOrdering.nullCompare(x.asInstanceOf[Comparable[Any]], y)
  }

  /**
    * Configure the iterator
    */
  def configure(stats: GeoMesaStats,
                sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                hints: Hints,
                skipSort: Boolean = false): ArrowScanConfig = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._

    val arrowSft = hints.getTransformSchema.getOrElse(sft)
    val includeFids = hints.isArrowIncludeFid
    val sort = hints.getArrowSort
    val batchSize = getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(includeFids)

    val baseConfig = {
      val base = AggregatingScan.configure(sft, index, filter, hints.getTransform, hints.getSampling)
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
      val config = baseConfig + (DictionaryKey -> encodeDictionaries(dictionaries))
      val reduce = mergeBatches(arrowSft, dictionaries, encoding, batchSize, sort)
      ArrowScanConfig(config, Some(reduce))
    } else if (hints.isArrowMultiFile) {
      val config = baseConfig ++ Map(
        DictionaryFieldsKey -> dictionaryFields.mkString(","),
        MultiFileKey        -> "true"
      )
      val reduce = mergeFiles(arrowSft, dictionaryFields, encoding, sort)
      ArrowScanConfig(config, Some(reduce))
    } else {
      val config = baseConfig + (DictionaryFieldsKey -> dictionaryFields.mkString(","))
      val reduce = mergeBatchesAndDictionaries(arrowSft, dictionaryFields, encoding, batchSize, sort)
      ArrowScanConfig(config, Some(reduce))
    }
  }

  def mergeFiles(sft: SimpleFeatureType,
                 dictionaryFields: Seq[String],
                 encoding: SimpleFeatureEncoding,
                 sort: Option[(String, Boolean)]):
      CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {
    // we don't need to manipulate anything, just return the file batches from the distributed scan
    (iter) => {
      // ensure that we return something
      if (iter.hasNext) { iter } else {
        val bytes = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
        createFile(sft, createEmptyDictionaries(dictionaryFields), encoding, sort)(bytes)
      }
    }
  }

  /**
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
                   sort: Option[(String, Boolean)]):
      CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {
    def sorted(iter: CloseableIterator[SimpleFeature]): CloseableIterator[Array[Byte]] = {
      val bytes = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
      if (sort.isEmpty) { bytes } else {
        val (attribute, reverse) = sort.get
        SimpleFeatureArrowIO.sortBatches(sft, dictionaries, encoding, attribute, reverse, batchSize, bytes)
      }
    }
    (iter) => createFile(sft, dictionaries, encoding, sort)(sorted(iter))
  }

  /**
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
                                  sort: Option[(String, Boolean)]):
      CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {

    // merge the files coming back into a single file with batches
    (iter) => {
      // note: get bytes before expanding to array as the simple feature may be re-used
      val files = try { iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]]).toArray } finally { iter.close() }
      if (files.isEmpty) {
        // ensure that we return something
        createFile(sft, createEmptyDictionaries(dictionaryFields), encoding, sort)(CloseableIterator.empty)
      } else if (files.length == 1) {
        // if only a single batch, we can just return it
        CloseableIterator(files.iterator.map(toFeature))
      } else {
        val sf = toFeature(null)
        val result = SimpleFeatureArrowIO.mergeFiles(sft, files, dictionaryFields, encoding, sort, batchSize)
        result.map { bytes => sf.setAttribute(0, bytes); sf }
      }
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
  private def createDictionaries(stats: GeoMesaStats,
                                 sft: SimpleFeatureType,
                                 filter: Option[Filter],
                                 attributes: Seq[String],
                                 provided: Map[String, Array[AnyRef]],
                                 cached: Map[String, TopK[AnyRef]]): Map[String, ArrowDictionary] = {
    val cached = cachedDictionaries.map { case (k, v) => (k, v.topK(DictionaryTopK.get.toInt).map(_._1).toArray) }
    var id = -1L
    if (attributes.isEmpty) { Map.empty } else {
      // note: sort values to return same dictionary cache
      val providedDictionaries = provided.map { case (k, v) => id += 1; sort(v); k -> ArrowDictionary.create(id, v) }
      val toLookup = attributes.filterNot(provided.contains)
      val queriedDictionaries = if (toLookup.isEmpty) { Map.empty } else {
        // use topk if available, otherwise run a live stats query to get the dictionary values
        val cached = if (useCached) {
          def name(i: Int): String = sft.getDescriptor(i).getLocalName
          stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => name(k.attribute) -> k).toMap
        } else {
          Map.empty[String, TopK[AnyRef]]
        }
        if (toLookup.forall(cached.contains)) {
          cached.map { case (name, k) =>
            id += 1
            val values = k.topK(1000).map(_._1).toArray
            sort(values)
            name -> ArrowDictionary.create(id, values)
          }
        } else {
          // if we have to run a query, might as well generate all values
          val query = Stat.SeqStat(toLookup.map(Stat.Enumeration))
          val enumerations = stats.runStats[EnumerationStat[String]](sft, query, filter.getOrElse(Filter.INCLUDE))
          // enumerations should come back in the same order
          // we can't use the enumeration attribute number b/c it may reflect a transform sft
          val nameIter = toLookup.iterator
          enumerations.map { e =>
            id += 1
            val values = e.values.toArray[AnyRef]
            sort(values)
            nameIter.next -> ArrowDictionary.create(id, values)
          }.toMap
        }
      }
      providedDictionaries ++ queriedDictionaries
    }
  }

  private def createEmptyDictionaries(fields: Seq[String]): Map[String, ArrowDictionary] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike
    fields.mapWithIndex { case (name, i) => name -> ArrowDictionary.create(i, Array.empty) }.toMap
  }

  private def createFile(sft: SimpleFeatureType,
                         dictionaries: Map[String, ArrowDictionary],
                         encoding: SimpleFeatureEncoding,
                         sort: Option[(String, Boolean)])
                        (body: CloseableIterator[Array[Byte]]): CloseableIterator[SimpleFeature] = {
    val sf = toFeature(null)
    val header = Iterator.single(toFeature(fileMetadata(sft, dictionaries, encoding, sort)))
    // per arrow streaming format footer is the encoded int '0'
    val footer = Iterator.single(toFeature(Array[Byte](0, 0, 0, 0)))
    CloseableIterator(header ++ body.map { b => sf.setAttribute(0, b); sf } ++ footer, body.close())
  }

  /**
    * Creates the header for the arrow file, which includes the schema and any dictionaries
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding options
    * @param sort data is sorted or not
    * @return
    */
  private def fileMetadata(sft: SimpleFeatureType,
                           dictionaries: Map[String, ArrowDictionary],
                           encoding: SimpleFeatureEncoding,
                           sort: Option[(String, Boolean)]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    WithClose(new SimpleFeatureArrowFileWriter(sft, out, dictionaries, encoding, sort)) { writer =>
      writer.start()
      out.toByteArray // copy bytes before closing so we get just the header metadata
    }
  }

  private def getBatchSize(hints: Hints): Int =
    hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)

  private def toFeature(b: Array[Byte]): SimpleFeature =
    new ScalaSimpleFeature(ArrowEncodedSft, "", Array(b, GeometryUtils.zeroPoint))

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
      k -> ArrowDictionary.create(id, v.toArray)
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
  }


  /**
    * Returns full arrow files, with metadata. Builds dictionaries on the fly. Doesn't sort
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries fields
    * @param encoding encoding
    */
  class FileAggregate(sft: SimpleFeatureType, dictionaryFields: Seq[String], encoding: SimpleFeatureEncoding)
      extends ArrowAggregate {

    import org.locationtech.geomesa.arrow.allocator

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

  /**
    * Returns record batches without any metadata. Dictionaries must be known up front. Doesn't sort
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding
    */
  class BatchAggregate(sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary], encoding: SimpleFeatureEncoding)
      extends ArrowAggregate {

    import org.locationtech.geomesa.arrow.allocator

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
                              sortField: Int,
                              reverse: Boolean) extends ArrowAggregate {

    import org.locationtech.geomesa.arrow.allocator

    private var index = 0
    private var features: Array[SimpleFeature] = _

    private val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    private val batchWriter = new RecordBatchUnloader(vector)

    private val ordering = if (reverse) { SimpleFeatureOrdering(sortField).reverse } else { SimpleFeatureOrdering(sortField) }

    override def withBatchSize(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim[SimpleFeature](size)
      }
      this
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

    private val os = new ByteArrayOutputStream()

    private val ordering = sort.map { case (name, reverse) =>
      val o = SimpleFeatureOrdering(sft.indexOf(name))
      if (reverse) { o.reverse } else { o }
    }

    override def withBatchSize(size: Int): ArrowAggregate = {
      if (features == null || features.length < size) {
        features = Array.ofDim[SimpleFeature](size)
      }
      this
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

  case class ArrowScanConfig(config: Map[String, String],
                             reduce: Option[CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]])
}