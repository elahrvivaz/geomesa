/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Collections

import com.google.common.collect.HashBiMap
import com.google.common.primitives.{Ints, Longs}
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Schema}
import org.apache.arrow.vector.util.TransferPair
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.math.Ordering

object SimpleFeatureArrowIO extends LazyLogging {

  import org.locationtech.geomesa.arrow.allocator

  object Metadata {
    val SortField = "sort-field"
    val SortOrder = "sort-order"
  }

  private val ordering = new Ordering[(AnyRef, Int, Int)] {
    override def compare(x: (AnyRef, Int, Int), y: (AnyRef, Int, Int)): Int =
      SimpleFeatureOrdering.nullCompare(x._1.asInstanceOf[Comparable[Any]], y._1)
  }

  /**
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding simple feature encoding
    * @param sort sort
    * @param files files
    * @return
    */
  def concatFiles(sft: SimpleFeatureType,
                  dictionaryFields: Seq[String],
                  encoding: SimpleFeatureEncoding,
                  sort: Option[(String, Boolean)])
                 (files: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    // ensure we return something
    if (files.hasNext) { files } else {
      // files is empty but this will pass it through to be closed
      createFile(sft, createEmptyDictionaries(dictionaryFields), encoding, sort)(files)
    }
  }

  /**
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding simple feature encoding
    * @param sort sort
    * @param batchSize batch size
    * @param batches record batches
    * @return
    */
  def mergeBatches(sft: SimpleFeatureType,
                   dictionaries: Map[String, ArrowDictionary],
                   encoding: SimpleFeatureEncoding,
                   sort: Option[(String, Boolean)],
                   batchSize: Int)
                  (batches: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    val sorted = sort match {
      case None => batches
      case Some((field, reverse)) => sortBatches(sft, dictionaries, encoding, field, reverse, batchSize, batches)
    }
    createFile(sft, dictionaries, encoding, sort)(sorted)
  }

  /**
    * Merges multiple arrow files into a single file
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries
    * @param encoding encoding
    * @param sort sort
    * @param batchSize record batch size
    * @param files full arrow files encoded per streaming format
    * @return
    */
  def mergeFiles(sft: SimpleFeatureType,
                 dictionaryFields: Seq[String],
                 encoding: SimpleFeatureEncoding,
                 sort: Option[(String, Boolean)],
                 batchSize: Int)
                (files: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {

    val bytes = try { files.toSeq } finally { files.close() }

    if (bytes.isEmpty) {
      // ensure that we return something
      createFile(sft, createEmptyDictionaries(dictionaryFields), encoding, sort)(CloseableIterator.empty)
    } else if (bytes.length == 1) {
      // if only a single batch, we can just return it
      CloseableIterator(bytes.iterator)
    } else {
      val readers = bytes.map(b => SimpleFeatureArrowFileReader.caching(new ByteArrayInputStream(b)))

      val MergedDictionaries(dictionaries, mappings) = mergeDictionaries(dictionaryFields, readers)

      sort match {
        case None => mergeUnsortedFiles(sft, dictionaries, readers, mappings, encoding)
        case Some((field, reverse)) => mergeSortedFiles(sft, dictionaries, readers, mappings, encoding, field, reverse, batchSize)
      }
    }
  }

  /**
    * Merges files without sorting
    */
  private def mergeUnsortedFiles(sft: SimpleFeatureType,
                                 dictionaries: Map[String, ArrowDictionary],
                                 readers: Seq[SimpleFeatureArrowFileReader],
                                 dictionaryMappings: Seq[Map[String, scala.collection.Map[Int, Int]]],
                                 encoding: SimpleFeatureEncoding): CloseableIterator[Array[Byte]] = {
    import scala.collection.JavaConversions._

    val result = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val unloader = new RecordBatchUnloader(result)

    val batches = readers.iterator.zip(dictionaryMappings.iterator).map { case (reader, mappings) =>
      result.clear()
      val from = reader.vectors.head.underlying // should only be a single reader vector per batch
      val transfers: Seq[(Int) => Unit] = from.getChildrenFromFields.map { child =>
        val to = result.underlying.getChild(child.getField.getName)
        if (child.getField.getDictionary == null) {
          val transfer = child.makeTransferPair(to)
          (index: Int) => transfer.copyValueSafe(index, index)
        } else {
          val accessor = child.getAccessor
          val mapping = mappings(child.getField.getName)
          to.getMutator match {
            case m: NullableTinyIntVector#Mutator => (index: Int) => {
              val n = accessor.getObject(index)
              if (n == null) {
                m.setNull(index)
              } else {
                m.setSafe(index, mapping(n.asInstanceOf[Number].intValue).toByte)
              }
            }
            case m: NullableSmallIntVector#Mutator => (index: Int) => {
              val n = accessor.getObject(index)
              if (n == null) {
                m.setNull(index)
              } else {
                m.setSafe(index, mapping(n.asInstanceOf[Number].intValue).toShort)
              }
            }
            case m: NullableIntVector#Mutator => (index: Int) => {
              val n = accessor.getObject(index)
              if (n == null) {
                m.setNull(index)
              } else {
                m.setSafe(index, mapping(n.asInstanceOf[Number].intValue))
              }
            }
            case m => throw new IllegalStateException(s"Expected NullableIntVector#Mutator, got $m")
          }
        }
      }
      var j = 0
      while (j < reader.vectors.head.reader.getValueCount) {
        transfers.foreach(_.apply(j))
        result.underlying.getMutator.setIndexDefined(j)
        j += 1
      }
      unloader.unload(j)
    }

    CloseableIterator(createFile(sft, dictionaries, encoding, None)(batches), readers.foreach(CloseWithLogging.apply))
  }

  /**
    * Merges files with sorting
    */
  private def mergeSortedFiles(sft: SimpleFeatureType,
                               dictionaries: Map[String, ArrowDictionary],
                               readers: Seq[SimpleFeatureArrowFileReader],
                               dictionaryMappings: Seq[Map[String, scala.collection.Map[Int, Int]]],
                               encoding: SimpleFeatureEncoding,
                               sortBy: String,
                               reverse: Boolean,
                               batchSize: Int): CloseableIterator[Array[Byte]] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

    import scala.collection.JavaConversions._

    // gets the attribute we're sorting by from the i-th feature in the vector
    val getSortAttribute: (SimpleFeatureVector, Int) => AnyRef = {
      val sortByIndex = sft.indexOf(sortBy)
      if (dictionaries.contains(sortBy)) {
        // since we've sorted the dictionaries, we can just compare the encoded index values
        (vector, i) => vector.reader.readers(sortByIndex).asInstanceOf[ArrowDictionaryReader].getEncoded(i)
      } else {
        (vector, i) => vector.reader.readers(sortByIndex).apply(i)
      }
    }

    val result = SimpleFeatureVector.create(sft, dictionaries, encoding)

    // TODO array builder instead of map + .toArray
    val inputs = readers.mapWithIndex { case (reader, readerIndex) =>
      val vector = reader.vectors.head // should only be a single reader vector per batch
      val mappings = dictionaryMappings(readerIndex)
      val transfers: Seq[(Int, Int) => Unit] = vector.underlying.getChildrenFromFields.map { child =>
        val to = result.underlying.getChild(child.getField.getName)
        if (child.getField.getDictionary == null) {
          val transfer = child.makeTransferPair(to)
          (fromIndex: Int, toIndex: Int) => transfer.copyValueSafe(fromIndex, toIndex)
        } else {
          val mapping = mappings(child.getField.getName)
          val accessor = child.getAccessor
          to.getMutator match {
            case m: NullableTinyIntVector#Mutator => (fromIndex: Int, toIndex: Int) => {
              val n = accessor.getObject(fromIndex)
              if (n == null) {
                m.setNull(toIndex)
              } else {
                m.setSafe(toIndex, mapping(n.asInstanceOf[Number].intValue).toByte)
              }
            }
            case m: NullableSmallIntVector#Mutator => (fromIndex: Int, toIndex: Int) => {
              val n = accessor.getObject(fromIndex)
              if (n == null) {
                m.setNull(toIndex)
              } else {
                m.setSafe(toIndex, mapping(n.asInstanceOf[Number].intValue).toShort)
              }
            }
            case m: NullableIntVector#Mutator => (fromIndex: Int, toIndex: Int) => {
              val n = accessor.getObject(fromIndex)
              if (n == null) {
                m.setNull(toIndex)
              } else {
                m.setSafe(toIndex, mapping(n.asInstanceOf[Number].intValue))
              }
            }
            case m => throw new IllegalStateException(s"Expected NullableIntVector#Mutator, got $m")
          }
        }
      }
      (vector, transfers)
    }.toArray

    // we do a merge sort of each batch
    // sorted queue of [(current batch value, current index in that batch, number of the batch)]
    val queue = {
      // populate with the first element from each batch
      // note: need to flip ordering here as highest sorted values come off the queue first
      val order = if (reverse) { ordering } else { ordering.reverse }
      val heads = scala.collection.mutable.PriorityQueue.empty[(AnyRef, Int, Int)](order)
      var i = 0
      while (i < inputs.length) {
        val vector = inputs(i)._1
        if (vector.reader.getValueCount > 0) {
          heads.+=((getSortAttribute(vector, 0), 0, i))
        } else {
          vector.close()
        }
        i += 1
      }
      heads
    }

    val unloader = new RecordBatchUnloader(result)

    // gets the next record batch to write - returns null if no further records
    def nextBatch(): Array[Byte] = {
      if (queue.isEmpty) { null } else {
        result.clear()
        var resultIndex = 0
        // copy the next sorted value and then queue and sort the next element out of the batch we copied from
        while (queue.nonEmpty && resultIndex < batchSize) {
          val (_, i, batch) = queue.dequeue()
          val (vector, transfers) = inputs(batch)
          transfers.foreach(_.apply(i, resultIndex))
          result.underlying.getMutator.setIndexDefined(resultIndex)
          resultIndex += 1
          val nextBatchIndex = i + 1
          if (vector.reader.getValueCount > nextBatchIndex) {
            val value = getSortAttribute(vector, nextBatchIndex)
            queue.+=((value, nextBatchIndex, batch))
          } else {
            vector.close()
          }
        }
        unloader.unload(resultIndex)
      }
    }

    val batches = new CloseableIterator[Array[Byte]] {
      private var batch: Array[Byte] = _

      override def hasNext: Boolean = {
        if (batch == null) {
          batch = nextBatch()
        }
        batch != null
      }

      override def next(): Array[Byte] = {
        val res = batch
        batch = null
        res
      }

      override def close(): Unit = {
        CloseWithLogging(result)
        readers.foreach(r => CloseWithLogging(r))
        inputs.foreach(i => CloseWithLogging(i._1))
      }
    }

    createFile(sft, dictionaries, encoding, None)(batches)
  }

  private def mergeDictionaries(dictionaryFields: Seq[String],
                                readers: Seq[SimpleFeatureArrowFileReader]): MergedDictionaries = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

    val toMerge: Array[Map[String, ArrowDictionary]] = readers.map { reader =>
      dictionaryFields.map(f => f -> reader.dictionaries(f)).toMap
    }.toArray

    val mappings = toMerge.map(d => d.map { case (f, _) => f -> scala.collection.mutable.Map.empty[Int, Int] })

    val dictionaries = dictionaryFields.mapWithIndex { case (field, dictionaryId) =>
      var maxDictionaryLength = 0
      val dictionaries: Array[Iterator[AnyRef]] = toMerge.map { dicts =>
        val dict = dicts(field)
        if (dict.length > maxDictionaryLength) {
          maxDictionaryLength = dict.length
        }
        dict.iterator
      }
      val mapping: Array[scala.collection.mutable.Map[Int, Int]] = mappings.map(_.apply(field))

      // estimate 150% for the initial array size and grow later if needed
      var values = Array.ofDim[AnyRef]((maxDictionaryLength * 1.5).toInt)
      var count = 0

      // we do a merge sort of each batch
      // sorted queue of [(current batch value, current index in that batch, number of the batch)]
      val queue = {
        // populate with the first element from each batch
        // note: need to flip ordering here as highest sorted values come off the queue first
        val heads = scala.collection.mutable.PriorityQueue.empty[(AnyRef, Int, Int)](ordering.reverse)
        var i = 0
        while (i < dictionaries.length) {
          val iterator = dictionaries(i)
          if (iterator.hasNext) {
            heads.+=((iterator.next, 0, i))
          }
          i += 1
        }
        heads
      }

      // copy the next sorted value and then queue and sort the next element out of the batch we copied from
      while (queue.nonEmpty) {
        val (value, i, batch) = queue.dequeue()
        if (count == 0 || values(count - 1) != value) {
          if (count == values.length - 1) {
            val tmp = Array.ofDim[AnyRef](values.length * 2)
            System.arraycopy(values, 0, tmp, 0, count)
            values = tmp
          }
          values(count) = value.asInstanceOf[AnyRef]
          count += 1
        }
        mapping(batch).put(i, count - 1)
        val iterator = dictionaries(batch)
        if (iterator.hasNext) {
          queue.+=((iterator.next, i + 1, batch))
        }
      }

      field -> ArrowDictionary.create(dictionaryId, values, count)
    }.toMap

    MergedDictionaries(dictionaries, mappings)
  }

  /**
    *
    * @param sft
    * @param dictionaryFields
    * @param encoding
    * @param sort
    * @param batchSize
    * @param deltas
    * @return
    */
  def mergeDeltas(sft: SimpleFeatureType,
                  dictionaryFields: Seq[String],
                  encoding: SimpleFeatureEncoding,
                  sort: Option[(String, Boolean)],
                  batchSize: Int)
                 (deltas: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {

    val threaded = try { deltas.toArray.groupBy(Longs.fromByteArray).values.toArray } finally { deltas.close() }

    logger.trace(s"merging ${threaded.length} threaded delta batches")

    val dictionaries = mergeDictionaryDeltas(sft, dictionaryFields, threaded, encoding)

    sort match {
      case None => mergeUnsortedDeltas(sft, dictionaryFields, encoding, dictionaries, batchSize, threaded)
      case Some((field, reverse)) =>
        mergeSortedDeltas(sft, dictionaryFields, encoding, dictionaries, field, reverse, batchSize, threaded)
    }
  }

  private def mergeUnsortedDeltas(sft: SimpleFeatureType,
                                  dictionaryFields: Seq[String],
                                  encoding: SimpleFeatureEncoding,
                                  mergedDictionaries: MergedDictionaryDeltas,
                                  batchSize: Int,
                                  threadedBatches: Array[Array[Array[Byte]]]): CloseableIterator[Array[Byte]] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    import scala.collection.JavaConversions._

    val MergedDictionaryDeltas(dictionaries, dictionaryMappings) = mergedDictionaries

    val result = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val unloader = new RecordBatchUnloader(result)

    logger.trace(s"merge unsorted deltas - read schema ${result.underlying.getField}")

    // TODO merge batches to hit batch size
    var batchIndex = -1
    val iter = threadedBatches.iterator.flatMap { batches =>
      batchIndex += 1
      val mappings = dictionaryMappings.map { case (f, m) => (f, m(batchIndex)) }
      batches.iterator.mapWithIndex { case (batch, j) =>
        // note: for some reason we have to allow the batch loader to create the vectors or this doesn't work
        val loader = RecordBatchLoader(result.underlying.getField)
        try {
          // skip the dictionary batches
          var offset = 8
          dictionaryFields.foreach { _ =>
            offset += Ints.fromBytes(batch(offset), batch(offset + 1), batch(offset + 2), batch(offset + 3)) + 4
          }
          val messageLength = Ints.fromBytes(batch(offset), batch(offset + 1), batch(offset + 2), batch(offset + 3))
          offset += 4
          // load the record batch
          loader.load(batch, offset, messageLength)
          val count = loader.vector.getAccessor.getValueCount
          logger.trace(s"merging batch with $count values")
          loader.vector.getChildrenFromFields.foreach { child =>
            val to = result.underlying.getChild(child.getField.getName)
            val transfer = if (child.getField.getDictionary == null) {
              val transfer = child.makeTransferPair(to)
              (fromIndex: Int, toIndex: Int) => transfer.copyValueSafe(fromIndex, toIndex)
            } else {
              val mapping = mappings(child.getField.getName)
              logger.trace(s"dictionary mappings: $mapping")
              val accessor = child.getAccessor
              val m = to.getMutator.asInstanceOf[NullableIntVector#Mutator]
              (fromIndex: Int, toIndex: Int) => {
                val n = accessor.getObject(fromIndex).asInstanceOf[Integer]
                if (n == null) {
                  m.setNull(toIndex)
                } else {
                  m.setSafe(toIndex, mapping(n))
                }
              }
            }
            var i = 0
            while (i < count) {
              transfer(i, i)
              i += 1
            }
            to.getMutator.setValueCount(count)
          }

          unloader.unload(count)
        } finally {
          loader.vector.close()
        }
      }
    }

    createFile(result, None)(CloseableIterator(iter, result.close()))
  }

  private def mergeSortedDeltas(sft: SimpleFeatureType,
                                dictionaryFields: Seq[String],
                                encoding: SimpleFeatureEncoding,
                                mergedDictionaries: MergedDictionaryDeltas,
                                sortBy: String,
                                reverse: Boolean,
                                batchSize: Int,
                                threadedBatches: Array[Array[Array[Byte]]]): CloseableIterator[Array[Byte]] = {

    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichArray

    import scala.collection.JavaConversions._

    val MergedDictionaryDeltas(dictionaries, dictionaryMappings) = mergedDictionaries

    // gets the attribute we're sorting by from the i-th feature in the vector
    val getSortAttribute: (NullableMapVector, scala.collection.Map[Integer, Integer], Int) => AnyRef = {
      // TODO look up child by index?
      if (dictionaries.contains(sortBy)) {
        // since we've sorted the dictionaries, we can just compare the encoded index values
        (vector, mappings, i) => {
          mappings.get(vector.getChild(sortBy).asInstanceOf[NullableIntVector].getAccessor.getObject(i))
        }
      } else {
        (vector, _, i) => vector.getChild(sortBy).getAccessor.getObject(i)
      }
    }

    val result = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val unloader = new RecordBatchUnloader(result)

    logger.trace(s"merging sorted deltas - read schema: ${result.underlying.getField}")

    val mergeBuilder = Array.newBuilder[(NullableMapVector, Seq[(Int, Int) => Unit], scala.collection.Map[Integer, Integer])]
    mergeBuilder.sizeHint(threadedBatches.foldLeft(0)((sum, a) => sum + a.length))

    threadedBatches.foreachIndex { case (batches, batchIndex) =>
      val mappings = dictionaryMappings.map { case (f, m) => (f, m(batchIndex)) }
      var j = 0
      logger.trace(s"loading ${batches.length} threaded batches")
      while (j < batches.length) {
        val batch = batches(j)
        // note: for some reason we have to allow the batch loader to create the vectors or this doesn't work
        val loader = RecordBatchLoader(result.underlying.getField)
        // skip the dictionary batches
        var offset = 8
        dictionaryFields.foreach { _ =>
          offset += Ints.fromBytes(batch(offset), batch(offset + 1), batch(offset + 2), batch(offset + 3)) + 4
        }
        val messageLength = Ints.fromBytes(batch(offset), batch(offset + 1), batch(offset + 2), batch(offset + 3))
        offset += 4
        // load the record batch
        loader.load(batch, offset, messageLength)
        val transfers: Seq[(Int, Int) => Unit] = loader.vector.getChildrenFromFields.map { child =>
          val to = result.underlying.getChild(child.getField.getName)
          if (child.getField.getDictionary == null) {
            val transfer = child.makeTransferPair(to)
            (fromIndex: Int, toIndex: Int) => transfer.copyValueSafe(fromIndex, toIndex)
          } else {
            val mapping = mappings(child.getField.getName)
            val accessor = child.getAccessor
            val m = to.getMutator.asInstanceOf[NullableIntVector#Mutator]
            (fromIndex: Int, toIndex: Int) => {
              val n = accessor.getObject(fromIndex).asInstanceOf[Integer]
              if (n == null) {
                m.setNull(toIndex)
              } else {
                m.setSafe(toIndex, mapping(n))
              }
            }
          }
        }
        mergeBuilder.+=((loader.vector.asInstanceOf[NullableMapVector], transfers, mappings.get(sortBy).orNull))
        j += 1
      }
    }

    val toMerge = mergeBuilder.result()

    // we do a merge sort of each batch
    // sorted queue of [(current batch value, current index in that batch, number of the batch)]
    // note: need to flip ordering here as highest sorted values come off the queue first
    val queue = scala.collection.mutable.PriorityQueue.empty[(AnyRef, Int, Int)](if (reverse) { ordering } else { ordering.reverse })

    toMerge.foreachIndex { case ((vector, _, mappings), i) =>
      if (vector.getAccessor.getValueCount > 0) {
        queue.+=((getSortAttribute(vector, mappings, 0), 0, i))
      }
    }

    // gets the next record batch to write - returns null if no further records
    def nextBatch(): Array[Byte] = {
      if (queue.isEmpty) { null } else {
        result.clear()
        var resultIndex = 0
        // copy the next sorted value and then queue and sort the next element out of the batch we copied from
        do {
          val (_, i, batch) = queue.dequeue()
          val (vector, transfers, mappings) = toMerge(batch)
          transfers.foreach(_.apply(i, resultIndex))
          result.underlying.getMutator.setIndexDefined(resultIndex)
          resultIndex += 1
          val nextBatchIndex = i + 1
          if (vector.getAccessor.getValueCount > nextBatchIndex) {
            val value = getSortAttribute(vector, mappings, nextBatchIndex)
            queue.+=((value, nextBatchIndex, batch))
          }
        } while (queue.nonEmpty && resultIndex < batchSize)
        unloader.unload(resultIndex)
      }
    }

    val merged = new CloseableIterator[Array[Byte]] {
      private var batch: Array[Byte] = _

      override def hasNext: Boolean = {
        if (batch == null) {
          batch = nextBatch()
        }
        batch != null
      }

      override def next(): Array[Byte] = {
        val res = batch
        batch = null
        res
      }

      override def close(): Unit = {
        CloseWithLogging(result)
        toMerge.foreach { case (vector, _, _) => CloseWithLogging(vector) }
      }
    }

    createFile(result, Some(sortBy, reverse))(merged)
  }

  /**
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param deltas Seq of threaded dictionary deltas
    * @return
    */
  private def mergeDictionaryDeltas(sft: SimpleFeatureType,
                                    dictionaryFields: Seq[String],
                                    deltas: Array[Array[Array[Byte]]],
                                    encoding: SimpleFeatureEncoding): MergedDictionaryDeltas = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.{RichArray, RichTraversableOnce}

    // create a vector for each dictionary field
    def createNewVectors: Array[ArrowAttributeReader] = {
      val builder = Array.newBuilder[ArrowAttributeReader]
      builder.sizeHint(dictionaryFields.length)
      dictionaryFields.foreach { f =>
        val descriptor = sft.getDescriptor(f)
        // use the writer to create the appropriate child vector
        val vector = ArrowAttributeWriter(sft, descriptor, None, None, encoding).vector
        builder += ArrowAttributeReader(descriptor, vector, None, encoding)
      }
      builder.result
    }

    // final results
    val results = createNewVectors

    // merge each threaded delta vector into a single dictionary for that thread
    // Seq[(dictionary vector, transfers to result, batch delta mappings)]
    val allMerges: Array[(Array[ArrowAttributeReader], Array[TransferPair], Array[HashBiMap[Integer, Integer]])] = deltas.map { deltas =>
      // deltas are threaded batches containing partial dictionary vectors

      // per-dictionary vectors for our final merged results for this threaded batch
      val dictionaries = createNewVectors

      // the delta vectors, each sorted internally
      val toMerge: Array[(Array[ArrowAttributeReader], Array[TransferPair])] = deltas.map { bytes =>
        val vectors = createNewVectors // per-dictionary vectors from this batch

        var i = 0
        var offset = 8 // start after threading key
        while (i < dictionaries.length) {
          val length = Ints.fromBytes(bytes(offset), bytes(offset + 1), bytes(offset + 2), bytes(offset + 3))
          offset += 4 // increment past length
          if (length > 0) {
            RecordBatchLoader(vectors(i).vector).load(bytes, offset, length)
            offset += length
          }
          i += 1
        }
        logger.trace(s"dictionary deltas: ${vectors.map(v => (0 until v.getValueCount).map(v.apply).mkString(",")).mkString(";")}")
        val transfers = vectors.mapWithIndex { case (v, j) => v.vector.makeTransferPair(dictionaries(j).vector) }
        (vectors, transfers)
      }

      // batch(dict(count))
      val offsets: Array[Array[Int]] = Array.tabulate(toMerge.length) { batch =>
        var i = 0
        val offset = Array.fill(dictionaries.length)(0)
        while (i < batch) {
          toMerge(i)._1.foreachIndex { case (v, j) => offset(j) += v.getValueCount }
          i += 1
        }
        offset
      }

      val transfers = Array.ofDim[TransferPair](dictionaries.length)
      val mappings = Array.fill(dictionaries.length)(HashBiMap.create[Integer, Integer]())

      var i = 0 // dictionary field index
      while (i < dictionaries.length) {
        // queue per dictionary field - note: need to flip ordering here as high items come off the queue first
        val queue = scala.collection.mutable.PriorityQueue.empty[(AnyRef, Int, Int)](ordering.reverse)

        // set initial values in the sorting queue
        toMerge.foreachIndex { case ((vectors, _), batch) =>
          if (vectors(i).getValueCount > 0) {
            queue.+=((vectors(i).apply(0), 0, batch))
          } else {
            CloseWithLogging(vectors(i).vector)
          }
        }

        var dest = 0
        while (queue.nonEmpty) {
          val (_, j, batch) = queue.dequeue()
          val (vectors, transfers) = toMerge(batch)
          transfers(i).copyValueSafe(j, dest)
          mappings(i).put(offsets(batch)(i) + j, dest)
          if (j < vectors(i).getValueCount - 1) {
            queue.+=((vectors(i).apply(j + 1), j + 1, batch))
          } else {
            CloseWithLogging(vectors(i).vector)
          }
          dest += 1
        }
        dictionaries(i).vector.getMutator.setValueCount(dest)
        transfers(i) = dictionaries(i).vector.makeTransferPair(results(i).vector)
        i += 1
      }

      (dictionaries, transfers, mappings)
    }

    // now merge the separate threads together
    val mappings = Array.fill(results.length)(Array.fill(allMerges.length)(scala.collection.mutable.Map.empty[Integer, Integer]))

    results.foreachIndex { case (result, i) =>
      // sorted queue of dictionary values - note: need to flip ordering here as high items come off the queue first
      val queue = scala.collection.mutable.PriorityQueue.empty[(AnyRef, Int, Int)](ordering.reverse)
      allMerges.foreachIndex { case ((vectors, _, _), batch) =>
        if (vectors(i).getValueCount > 0) {
          queue.+=((vectors(i).apply(0), batch, 0))
        } else {
          CloseWithLogging(vectors(i).vector)
        }
      }

      var dest = 0
      while (queue.nonEmpty) {
        val (tmp, batch, j) = queue.dequeue()
        val (vectors, transfers, mapping) = allMerges.apply(batch)
        if (dest > 0 && result.apply(dest - 1) == vectors(i).apply(j)) {
          // duplicate
          logger.trace(s"remap $tmp $batch ${mapping(i)} $j -> ${dest - 1}")
          val remap = mapping(i).inverse().get(j)
          if (remap != null) {
            mappings(i)(batch).put(remap, dest - 1)
          }
        } else {
          transfers(i).copyValueSafe(j, dest)
          logger.trace(s"remap $tmp $batch ${mapping(i)} $j -> $dest")
          val remap = mapping(i).inverse().remove(j)
          if (remap != null) {
            mappings(i)(batch).put(remap, dest)
          }
          dest += 1
        }
        if (j < vectors(i).getValueCount - 1) {
          queue.+=((vectors(i).apply(j + 1), batch, j + 1))
        } else {
          CloseWithLogging(vectors(i).vector)
        }
      }
      result.vector.getMutator.setValueCount(dest)
    }

    val dictionaryBuilder = Map.newBuilder[String, ArrowDictionary]
    dictionaryBuilder.sizeHint(dictionaryFields.length)
    val mappingsBuilder = Map.newBuilder[String, Array[scala.collection.Map[Integer, Integer]]]
    mappingsBuilder.sizeHint(dictionaryFields.length)

    dictionaryFields.foreachIndex { case (f, i) =>
      logger.trace("merged dictionary: " + (0 until results(i).getValueCount).map(results(i).apply).mkString(","))
      val encoding = new DictionaryEncoding(i, true, new ArrowType.Int(32, true))
      dictionaryBuilder.+=((f, ArrowDictionary.create(encoding, results(i).vector, sft.getDescriptor(f))))
      mappingsBuilder.+=((f, mappings(i).asInstanceOf[Array[scala.collection.Map[Integer, Integer]]]))
    }

    val dictionaryMap = dictionaryBuilder.result()
    val mappingsMap = mappingsBuilder.result()

    logger.trace(s"batch dictionary mappings: ${mappingsMap.mapValues(_.mkString(",")).mkString(";")}")
    MergedDictionaryDeltas(dictionaryMap, mappingsMap)
  }

  private case class MergedDictionaryDeltas(dictionaries: Map[String, ArrowDictionary],
                                            mappings: Map[String, Array[scala.collection.Map[Integer, Integer]]])

  private case class MergedDictionaries(dictionaries: Map[String, ArrowDictionary],
                                        mappings: Seq[Map[String, scala.collection.Map[Int, Int]]])

  /**
    * Sorts record batches. Batches are assumed to be already sorted.
    *
    * @param sft simple feature type
    * @param dictionaries dictionaries
    * @param encoding encoding options
    * @param sortBy attribute to sort by, assumed to be comparable
    * @param reverse sort reversed or not
    * @param batchSize batch size
    * @param iter iterator of batches
    * @return iterator of sorted batches
    */
  def sortBatches(sft: SimpleFeatureType,
                  dictionaries: Map[String, ArrowDictionary],
                  encoding: SimpleFeatureEncoding,
                  sortBy: String,
                  reverse: Boolean,
                  batchSize: Int,
                  iter: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    val batches = iter.toSeq
    if (batches.length < 2) {
      CloseableIterator(batches.iterator, iter.close())
    } else {
      // gets the attribute we're sorting by from the i-th feature in the vector
      val getSortAttribute: (SimpleFeatureVector, Int) => AnyRef = {
        val sortByIndex = sft.indexOf(sortBy)
        if (dictionaries.contains(sortBy)) {
          // since we've sorted the dictionaries, we can just compare the encoded index values
          (vector, i) => vector.reader.readers(sortByIndex).asInstanceOf[ArrowDictionaryReader].getEncoded(i)
        } else {
          (vector, i) => vector.reader.readers(sortByIndex).apply(i)
        }
      }

      val result = SimpleFeatureVector.create(sft, dictionaries, encoding)

      val inputs = batches.map { bytes =>
        // note: for some reason we have to allow the batch loader to create the vectors or this doesn't work
        val field = result.underlying.getField
        val loader = RecordBatchLoader(field)
        val vector = SimpleFeatureVector.wrap(loader.vector.asInstanceOf[NullableMapVector], dictionaries)
        loader.load(bytes)
        val transfer = vector.underlying.makeTransferPair(result.underlying)
        (vector, transfer)
      }.toArray

      // we do a merge sort of each batch
      // sorted queue of [(current batch value, current index in that batch, number of the batch)]
      val queue = {
        // populate with the first element from each batch
        // note: need to flip ordering here as highest sorted values come off the queue first
        val order = if (reverse) { ordering } else { ordering.reverse }
        val heads = scala.collection.mutable.PriorityQueue.empty[(AnyRef, Int, Int)](order)
        var i = 0
        while (i < inputs.length) {
          val vector = inputs(i)._1
          if (vector.reader.getValueCount > 0) {
            heads.+=((getSortAttribute(vector, 0), 0, i))
          } else {
            vector.close()
          }
          i += 1
        }
        heads
      }

      val unloader = new RecordBatchUnloader(result)

      // gets the next record batch to write - returns null if no further records
      def nextBatch(): Array[Byte] = {
        if (queue.isEmpty) { null } else {
          result.clear()
          var resultIndex = 0
          // copy the next sorted value and then queue and sort the next element out of the batch we copied from
          while (queue.nonEmpty && resultIndex < batchSize) {
            val (_, i, batch) = queue.dequeue()
            val (vector, transfer) = inputs(batch)
            transfer.copyValueSafe(i, resultIndex)
            resultIndex += 1
            val nextBatchIndex = i + 1
            if (vector.reader.getValueCount > nextBatchIndex) {
              val value = getSortAttribute(vector, nextBatchIndex)
              queue.+=((value, nextBatchIndex, batch))
            } else {
              vector.close()
            }
          }
          unloader.unload(resultIndex)
        }
      }

      val output = new Iterator[Array[Byte]] {
        private var batch: Array[Byte] = _

        override def hasNext: Boolean = {
          if (batch == null) {
            batch = nextBatch()
          }
          batch != null
        }

        override def next(): Array[Byte] = {
          val res = batch
          batch = null
          res
        }
      }

      def closeAll(): Unit = {
        CloseWithLogging(result)
        CloseWithLogging(iter)
        inputs.foreach(i => CloseWithLogging(i._1))
      }

      CloseableIterator(output, closeAll())
    }
  }

  def createFile(vector: SimpleFeatureVector,
                 sort: Option[(String, Boolean)])
                (body: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    // header with schema and dictionaries
    val headerBytes = new ByteArrayOutputStream
    // make sure to copy bytes before closing so we get just the header metadata
    val writer = SimpleFeatureArrowFileWriter(vector, headerBytes, sort)
    writer.start()

    val header = Iterator.single(headerBytes.toByteArray)
    // per arrow streaming format footer is the encoded int '0'
    val footer = Iterator.single(Array[Byte](0, 0, 0, 0))
    CloseableIterator(header ++ body ++ footer, { CloseWithLogging(body); CloseWithLogging(writer) })
  }

  def createFile(sft: SimpleFeatureType,
                 dictionaries: Map[String, ArrowDictionary],
                 encoding: SimpleFeatureEncoding,
                 sort: Option[(String, Boolean)])
                (body: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    createFile(SimpleFeatureVector.create(sft, dictionaries, encoding, 0), sort)(body)
  }

  /**
    * Checks schema metadata for sort fields
    *
    * @param metadata schema metadata
    * @return (sort field, reverse sorted or not)
    */
  def getSortFromMetadata(metadata: java.util.Map[String, String]): Option[(String, Boolean)] = {
    Option(metadata.get(Metadata.SortField)).map { field =>
      val reverse = Option(metadata.get(Metadata.SortOrder)).exists {
        case "descending" => true
        case _ => false
      }
      (field, reverse)
    }
  }

  /**
    * Creates metadata for sort fields
    *
    * @param field sort field
    * @param reverse reverse sorted or not
    * @return metadata map
    */
  def getSortAsMetadata(field: String, reverse: Boolean): java.util.Map[String, String] = {
    import scala.collection.JavaConversions._
    // note: reverse == descending
    Map(Metadata.SortField -> field, Metadata.SortOrder -> (if (reverse) { "descending" } else { "ascending" }))
  }

  def createRoot(vector: FieldVector, metadata: java.util.Map[String, String] = null): VectorSchemaRoot = {
    val schema = new Schema(Collections.singletonList(vector.getField), metadata)
    new VectorSchemaRoot(schema, Collections.singletonList(vector), vector.getAccessor.getValueCount)
  }

  private def createEmptyDictionaries(fields: Seq[String]): Map[String, ArrowDictionary] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike
    fields.mapWithIndex { case (name, i) => name -> ArrowDictionary.create(i, Array.empty[AnyRef]) }.toMap
  }
}
