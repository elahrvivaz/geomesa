/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{ByteArrayOutputStream, Closeable}
import java.nio.channels.Channels
import java.util.Collections

import com.google.common.primitives.{Ints, Longs}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.file.WriteChannel
import org.apache.arrow.vector.stream.{ArrowStreamWriter, MessageSerializer}
import org.apache.arrow.vector.types.pojo.Schema
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowIO.{MergedDictionaries, mergeDictionaries}
import org.locationtech.geomesa.arrow.io.records.RecordBatchLoader
import org.locationtech.geomesa.arrow.vector.{ArrowAttributeWriter, SimpleFeatureVector}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Random

object DeltaWriter {

  val DictionaryOrdering: Ordering[AnyRef] = new Ordering[AnyRef] {
    override def compare(x: AnyRef, y: AnyRef): Int =
      SimpleFeatureOrdering.nullCompare(x.asInstanceOf[Comparable[Any]], y)
  }

  private case class Writer(name: String,
                            index: Int,
                            attribute: ArrowAttributeWriter,
                            root: VectorSchemaRoot,
                            writer: ArrowStreamWriter,
                            os: ByteArrayOutputStream,
                            dictionary: Option[DictionaryWriter] = None)

  private case class DictionaryWriter(attribute: ArrowAttributeWriter,
                                      root: VectorSchemaRoot,
                                      writer: ArrowStreamWriter,
                                      os: ByteArrayOutputStream,
                                      values: scala.collection.mutable.Map[AnyRef, Integer])
}

class DeltaWriter(val sft: SimpleFeatureType,
                  dictionaryFields: Seq[String],
                  encoding: SimpleFeatureEncoding,
                  sort: Option[(String, Boolean)],
                  batchSize: Int) extends Closeable {

  import DeltaWriter._
  import org.locationtech.geomesa.arrow.allocator

  import scala.collection.JavaConversions._

  private val threadingKey = Random.nextLong()
  private val out = new ByteArrayOutputStream

  private val vector = NullableMapVector.empty(sft.getTypeName, allocator)

  // empty provider
  private val provider = new MapDictionaryProvider()

  private val ordering = sort.map { case (field, reverse) =>
    val o = SimpleFeatureOrdering(sft.indexOf(field))
    if (reverse) { o.reverse } else { o }
  }

  private val writers = sft.getAttributeDescriptors.map { descriptor =>
    val name = descriptor.getLocalName
    val isDictionary = dictionaryFields.contains(name)
    val classBinding = if (isDictionary) { classOf[Integer] } else { descriptor.getType.getBinding }
    val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
    val attribute = ArrowAttributeWriter(name, bindings.+:(objectType), classBinding, vector, None, Map.empty, encoding)
    val child = vector.getChild(name)
    val schema = new Schema(Collections.singletonList(child.getField))
    val root = new VectorSchemaRoot(schema, Collections.singletonList(child), 0)
    val os = new ByteArrayOutputStream // TODO re-use byte arrays?
    val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os))
    writer.start()
    os.reset() // discard the metadata, we only care about the record batches
    val dictionary = if (!isDictionary) { None } else {
      var i = 0
      var dictionaryName: String = null
      // ensure unique name
      do {
        dictionaryName = s"$name-dict-$i"
        i += 1
      } while (sft.indexOf(dictionaryName) != -1)
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      val attribute = ArrowAttributeWriter(dictionaryName, bindings.+:(objectType), classBinding, vector, None, Map.empty, encoding)
      val child = vector.getChild(dictionaryName)
      val schema = new Schema(Collections.singletonList(child.getField))
      val root = new VectorSchemaRoot(schema, Collections.singletonList(child), 0)
      val os = new ByteArrayOutputStream // TODO re-use byte arrays?
      val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os))
      writer.start()
      os.reset() // discard the metadata, we only care about the record batches
      Some(DictionaryWriter(attribute, root, writer, os, scala.collection.mutable.Map.empty))
    }
    Writer(name, sft.indexOf(name), attribute, root, writer, os, dictionary)
  }

  // set capacity after all child vectors have been created by the writers, then allocate
  vector.setInitialCapacity(batchSize)
  vector.allocateNew()

  /**
    * Clear any existing dictionary values
    */
  def reset(): Unit = writers.foreach(writer => writer.dictionary.foreach(_.values.clear()))

  /**
    * Writes out a record batch. Format is:
    *
    * 8 bytes long - threading key
    * (foreach dictionaryField) -> {
    *   4 byte int - length of dictionary batch
    *   anyref vector batch with dictionary delta values
    * }
    * (foreach field) -> {
    *   4 byte int - length of record batch
    *   anyref vector batch (may be dictionary encodings)
    * }
    *
    * Note: will sort the feature array in place if sorting is defined
    *
    * @param features features to write
    * @param count number of features to write, starting from array index 0
    * @return serialized record batch
    */
  def writeBatch(features: Array[SimpleFeature], count: Int): Array[Byte] = {

    out.reset()
    out.write(Longs.toByteArray(threadingKey))

    ordering.foreach(java.util.Arrays.sort(features, 0, count, _))

    dictionaryFields.foreach { field =>
      val attribute = sft.indexOf(field)
      val dictionary = writers.find(_.name == field).get.dictionary.get
      // come up with the delta of new dictionary values
      val delta = scala.collection.mutable.SortedSet.empty[AnyRef](DictionaryOrdering)
      var i = 0
      while (i < count) {
        val value = features(i).getAttribute(attribute)
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
      dictionary.root.setRowCount(count)
      dictionary.os.reset()
      dictionary.writer.writeBatch()
      out.write(Ints.toByteArray(dictionary.os.size()))
      dictionary.os.writeTo(out)
    }

    writers.foreach { writer =>
      var i = 0
      val getAttribute: () => AnyRef = writer.dictionary match {
        case None =>             () => features(i).getAttribute(writer.index)
        case Some(dictionary) => () => dictionary.values(features(i).getAttribute(writer.index))
      }
      while (i < count) {
        writer.attribute.apply(i, getAttribute)
        i += 1
      }
      writer.attribute.setValueCount(count)
      writer.root.setRowCount(count)
      writer.os.reset()
      writer.writer.writeBatch()
      out.write(Ints.toByteArray(writer.os.size()))
      writer.os.writeTo(out)
    }

    out.toByteArray
  }

  /**
    * Close the writer
    */
  override def close(): Unit = {
    writers.foreach { writer =>
      CloseWithLogging(writer.root)
      CloseWithLogging(writer.writer)
      writer.dictionary.foreach { dictionary =>
        CloseWithLogging(dictionary.root)
        CloseWithLogging(dictionary.writer)
      }
    }
    CloseWithLogging(vector)
  }
}
