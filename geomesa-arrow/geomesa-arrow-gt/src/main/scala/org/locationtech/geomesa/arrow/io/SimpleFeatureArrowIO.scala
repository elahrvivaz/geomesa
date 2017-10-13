/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.{NullableIntVector, NullableSmallIntVector, NullableTinyIntVector}
import org.locationtech.geomesa.arrow.io.records.{RecordBatchLoader, RecordBatchUnloader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, ArrowDictionaryReader, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.SimpleFeatureType

import scala.math.Ordering

object SimpleFeatureArrowIO {

  import org.locationtech.geomesa.arrow.allocator

  object Metadata {
    val SortField = "sort-field"
    val SortOrder = "sort-order"
  }

  private val ordering = new Ordering[(Comparable[Any], Int, Int)] {
    override def compare(x: (Comparable[Any], Int, Int), y: (Comparable[Any], Int, Int)): Int =
      x._1.compareTo(y._1)
  }

  // per arrow streaming format footer is the encoded int '0'
  private val footerBytes = Array[Byte](0, 0, 0, 0)

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

  def mergeFiles(sft: SimpleFeatureType,
                 files: Seq[Array[Byte]],
                 dictionaryFields: Seq[String],
                 encoding: SimpleFeatureEncoding,
                 sort: Option[(String, Boolean)],
                 batchSize: Int): CloseableIterator[Array[Byte]] = {
    val readers = files.map(bytes => SimpleFeatureArrowFileReader.caching(new ByteArrayInputStream(bytes)))

    val MergedDictionaries(dictionaries, mappings) = mergeDictionaries(dictionaryFields, readers)

    sort match {
      case None => mergeFiles(sft, dictionaries, readers, mappings, encoding)
      case Some((field, reverse)) => mergeSortedFiles(sft, dictionaries, readers, mappings, encoding, field, reverse, batchSize)
    }
  }

  private def mergeFiles(sft: SimpleFeatureType,
                         dictionaries: Map[String, ArrowDictionary],
                         readers: Seq[SimpleFeatureArrowFileReader],
                         dictionaryMappings: Seq[Map[String, scala.collection.Map[Int, Int]]],
                         encoding: SimpleFeatureEncoding): CloseableIterator[Array[Byte]] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    import scala.collection.JavaConversions._

    val result = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val unloader = new RecordBatchUnloader(result)

    val batches = readers.iterator.mapWithIndex { case (reader, i) =>
      result.clear()
      val mappings = dictionaryMappings(i)
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
            case m: NullableTinyIntVector#Mutator => (index: Int) => m.setSafe(index, mapping(accessor.getObject(index).asInstanceOf[Number].intValue).toByte)
            case m: NullableSmallIntVector#Mutator => (index: Int) => m.setSafe(index, mapping(accessor.getObject(index).asInstanceOf[Number].intValue).toShort)
            case m: NullableIntVector#Mutator => (index: Int) => m.setSafe(index, mapping(accessor.getObject(index).asInstanceOf[Number].intValue))
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

    val header = Iterator.single(fileMetadata(sft, dictionaries, encoding, None))
    val footer = Iterator.single(footerBytes)
    CloseableIterator(header ++ batches ++ footer, readers.foreach(CloseWithLogging.apply))
  }

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
    val getSortAttribute: (SimpleFeatureVector, Int) => Comparable[Any] = {
      val sortByIndex = sft.indexOf(sortBy)
      if (dictionaries.contains(sortBy)) {
        // since we've sorted the dictionaries, we can just compare the encoded index values
        (vector, i) => {
          val reader = vector.reader.readers(sortByIndex).asInstanceOf[ArrowDictionaryReader]
          reader.getEncoded(i).asInstanceOf[Comparable[Any]]
        }
      } else {
        (vector, i) => vector.reader.readers(sortByIndex).apply(i).asInstanceOf[Comparable[Any]]
      }
    }

    val result = SimpleFeatureVector.create(sft, dictionaries, encoding)

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
            case m: NullableTinyIntVector#Mutator => (fromIndex: Int, toIndex: Int) => m.setSafe(toIndex, mapping(accessor.getObject(fromIndex).asInstanceOf[Number].intValue).toByte)
            case m: NullableSmallIntVector#Mutator => (fromIndex: Int, toIndex: Int) => m.setSafe(toIndex, mapping(accessor.getObject(fromIndex).asInstanceOf[Number].intValue).toShort)
            case m: NullableIntVector#Mutator => (fromIndex: Int, toIndex: Int) => m.setSafe(toIndex, mapping(accessor.getObject(fromIndex).asInstanceOf[Number].intValue))
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
      val heads = scala.collection.mutable.PriorityQueue.empty[(Comparable[Any], Int, Int)](order)
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

    val batches = new Iterator[Array[Byte]] {
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
      readers.foreach(r => CloseWithLogging(r))
      inputs.foreach(i => CloseWithLogging(i._1))
    }

    val header = Iterator.single(fileMetadata(sft, dictionaries, encoding, None))
    val footer = Iterator.single(footerBytes)

    CloseableIterator(header ++ batches ++ footer, closeAll())
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
        val dict = dicts.apply(field)
        if (dict.length > maxDictionaryLength) {
          maxDictionaryLength = dict.length
        }
        dict.iterator
      }
      val mapping: Array[scala.collection.mutable.Map[Int, Int]] = mappings.map(_.apply(field))

      var values = Array.ofDim[AnyRef]((maxDictionaryLength * 1.5).toInt)
      var count = 0

      // we do a merge sort of each batch
      // sorted queue of [(current batch value, current index in that batch, number of the batch)]
      val queue = {
        // populate with the first element from each batch
        // note: need to flip ordering here as highest sorted values come off the queue first
        val heads = scala.collection.mutable.PriorityQueue.empty[(Comparable[Any], Int, Int)](ordering.reverse)
        var i = 0
        while (i < dictionaries.length) {
          val iterator = dictionaries(i)
          if (iterator.hasNext) {
            heads.+=((iterator.next.asInstanceOf[Comparable[Any]], 0, i))
          }
          i += 1
        }
        heads
      }

      // copy the next sorted value and then queue and sort the next element out of the batch we copied from
      while (queue.nonEmpty) {
        val (value, i, batch) = queue.dequeue()
        if (count == 0 || values(count) != value) {
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
          queue.+=((iterator.next.asInstanceOf[Comparable[Any]], i + 1, batch))
        }
      }

      field -> ArrowDictionary.create(dictionaryId, values, count)
    }.toMap

    MergedDictionaries(dictionaries, mappings)
  }

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
      val getSortAttribute: (SimpleFeatureVector, Int) => Comparable[Any] = {
        val sortByIndex = sft.indexOf(sortBy)
        if (dictionaries.contains(sortBy)) {
          // since we've sorted the dictionaries, we can just compare the encoded index values
          (vector, i) =>
            vector.reader.readers(sortByIndex).asInstanceOf[ArrowDictionaryReader].getEncoded(i).asInstanceOf[Comparable[Any]]
        } else {
          (vector, i) => vector.reader.readers(sortByIndex).apply(i).asInstanceOf[Comparable[Any]]
        }
      }

      val result = SimpleFeatureVector.create(sft, dictionaries, encoding)

      val inputs = batches.map { bytes =>
        // note: for some reason we have to allow the batch loader to create the vectors or this doesn't work
        val field = result.underlying.getField
        val loader = new RecordBatchLoader(field)
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
        val heads = scala.collection.mutable.PriorityQueue.empty[(Comparable[Any], Int, Int)](order)
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
}
