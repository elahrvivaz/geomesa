/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{ByteArrayOutputStream, Closeable, OutputStream}
import java.nio.channels.Channels
import java.util.concurrent.ThreadLocalRandom

import com.google.common.primitives.{Ints, Longs}
import com.typesafe.scalalogging.StrictLogging
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.stream.ArrowStreamWriter
import org.locationtech.geomesa.arrow.vector.ArrowAttributeWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object DeltaWriter extends StrictLogging {

  val DictionaryOrdering: Ordering[AnyRef] = new Ordering[AnyRef] {
    override def compare(x: AnyRef, y: AnyRef): Int =
      SimpleFeatureOrdering.nullCompare(x.asInstanceOf[Comparable[Any]], y)
  }

  // empty provider
  private val provider = new MapDictionaryProvider()

  private case class FieldWriter(name: String,
                                 index: Int,
                                 attribute: ArrowAttributeWriter,
                                 dictionary: Option[DictionaryWriter] = None)

  private case class DictionaryWriter(index: Int,
                                      attribute: ArrowAttributeWriter,
                                      writer: BatchWriter,
                                      values: scala.collection.mutable.Map[AnyRef, Integer])

  private class BatchWriter(vector: FieldVector) extends Closeable {

    private val root = SimpleFeatureArrowIO.createRoot(vector)
    private val os = new ByteArrayOutputStream()
    private val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os))
    writer.start() // start the writer - we'll discard the metadata later, as we only care about the record batches

    logger.trace(s"write schema: ${vector.getField}")

    def writeBatch(count: Int, to: OutputStream): Unit = {
      os.reset()
      if (count < 1) {
        logger.trace("writing 0 bytes")
        to.write(Ints.toByteArray(0))
      } else {
        vector.getMutator.setValueCount(count)
        root.setRowCount(count)
        writer.writeBatch()
        logger.trace(s"writing ${os.size} bytes")
        to.write(Ints.toByteArray(os.size())) // TODO we only need this for dictionary deltas not record batch...
        os.writeTo(to)
      }
    }

    override def close(): Unit = {
      CloseWithLogging(writer)
      CloseWithLogging(root)
    }
  }
}

class DeltaWriter(val sft: SimpleFeatureType,
                  dictionaryFields: Seq[String],
                  encoding: SimpleFeatureEncoding,
                  sort: Option[(String, Boolean)],
                  initialCapacity: Int) extends Closeable with StrictLogging {

  import DeltaWriter._
  import org.locationtech.geomesa.arrow.allocator

  import scala.collection.JavaConversions._

  private var threadingKey: Long = math.abs(ThreadLocalRandom.current().nextLong)
  logger.trace(s"$threadingKey created")

  private val result = new ByteArrayOutputStream

  private val vector = NullableMapVector.empty(sft.getTypeName, allocator)
  private val dictionaryVector = NullableMapVector.empty(sft.getTypeName, allocator)

  private val ordering = sort.map { case (field, reverse) =>
    val o = SimpleFeatureOrdering(sft.indexOf(field))
    if (reverse) { o.reverse } else { o }
  }

  private val idWriter = ArrowAttributeWriter.id(Some(vector), encoding)
  private val writers = sft.getAttributeDescriptors.map { descriptor =>
    val name = descriptor.getLocalName
    val isDictionary = dictionaryFields.contains(name)
    val classBinding = if (isDictionary) { classOf[Integer] } else { descriptor.getType.getBinding }
    val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
    val attribute = ArrowAttributeWriter(name, bindings.+:(objectType), classBinding, Some(vector), None, Map.empty, encoding)
    val dictionary = if (!isDictionary) { None } else {
      var i = 0
      // TODO ensure unique name? not sure it matters
      var dictionaryName: String = s"$name-dict-$i"
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      val attribute = ArrowAttributeWriter(dictionaryName, bindings.+:(objectType), classBinding, Some(dictionaryVector), None, Map.empty, encoding)
      val writer = new BatchWriter(dictionaryVector.getChild(dictionaryName))
      Some(DictionaryWriter(sft.indexOf(name), attribute, writer, scala.collection.mutable.Map.empty))
    }
    FieldWriter(name, sft.indexOf(name), attribute, dictionary)
  }

  private val dictionaryWriters = dictionaryFields.map(f => writers.find(_.name == f).get.dictionary.get)

  private val writer = new BatchWriter(vector)

  // set capacity after all child vectors have been created by the writers, then allocate
  Seq(vector, dictionaryVector).foreach { v =>
    v.setInitialCapacity(initialCapacity)
    v.allocateNew()
  }

  /**
    * Clear any existing dictionary values
    */
  def reset(): Unit = {
    val old = threadingKey
    threadingKey = math.abs(ThreadLocalRandom.current().nextLong)
    logger.trace(s"$old resetting to $threadingKey")
    writers.foreach(writer => writer.dictionary.foreach(_.values.clear()))
  }

  /**
    * Writes out a record batch. Format is:
    *
    * 8 bytes long - threading key
    * (foreach dictionaryField) -> {
    *   4 byte int - length of dictionary batch
    *   anyref vector batch with dictionary delta values
    * }
    * 4 byte int - length of record batch
    * record batch (may be dictionary encodings)
    *
    * Note: will sort the feature array in place if sorting is defined
    *
    * @param features features to write
    * @param count number of features to write, starting from array index 0
    * @return serialized record batch
    */
  def writeBatch(features: Array[SimpleFeature], count: Int): Array[Byte] = {

    result.reset()
    result.write(Longs.toByteArray(threadingKey))

    ordering.foreach(java.util.Arrays.sort(features, 0, count, _))

    dictionaryWriters.foreach { dictionary =>
      // come up with the delta of new dictionary values
      val delta = scala.collection.mutable.SortedSet.empty[AnyRef](DictionaryOrdering)
      var i = 0
      while (i < count) {
        val value = features(i).getAttribute(dictionary.index)
        if (!dictionary.values.contains(value)) {
          delta.add(value)
        }
        i += 1
      }
      val size = dictionary.values.size
      i = 0
      // update the dictionary mappings, and write the new values to the vector
      delta.foreach { n =>
        dictionary.values.put(n, i + size)
        dictionary.attribute.apply(i, n)
        i += 1
      }
      // write out the dictionary batch
      dictionary.attribute.setValueCount(i)
      logger.trace(s"$threadingKey writing dictionary delta with $i values")
      dictionary.writer.writeBatch(i, result)
    }

    if (encoding.fids) {
      var i = 0
      while (i < count) {
        idWriter.apply(i, features(i).getID)
        i += 1
      }
      idWriter.setValueCount(count)
    }

    writers.foreach { writer =>
      val getAttribute: (Int) => AnyRef = writer.dictionary match {
        case None =>             (i) => features(i).getAttribute(writer.index)
        case Some(dictionary) => (i) => dictionary.values(features(i).getAttribute(writer.index))
      }
      var i = 0
      while (i < count) {
        writer.attribute.apply(i, getAttribute(i))
        i += 1
      }
      writer.attribute.setValueCount(count)
    }

    logger.trace(s"$threadingKey writing batch with $count values")

    writer.writeBatch(count, result)

    result.toByteArray
  }

  /**
    * Close the writer
    */
  override def close(): Unit = {
    CloseWithLogging(writer)
    dictionaryWriters.foreach(w => CloseWithLogging(w.writer))
    CloseWithLogging(vector)
    CloseWithLogging(dictionaryVector)
  }
}
