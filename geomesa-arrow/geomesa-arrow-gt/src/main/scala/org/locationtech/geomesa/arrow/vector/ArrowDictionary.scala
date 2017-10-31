/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import com.vividsolutions.jts.geom.Geometry
import org.apache.arrow.vector.{FieldVector, NullableBigIntVector}
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.{EncodingPrecision, SimpleFeatureEncoding}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor

import scala.reflect.ClassTag

/**
  * Holder for dictionary values
  */
trait ArrowDictionary {

  def encoding: DictionaryEncoding
  def id: Long = encoding.getId
  def length: Int

  /**
    * Decode a dictionary int to a value. Note: may not be thread safe
    *
    * @param i dictionary encoded int
    * @return value
    */
  def lookup(i: Int): AnyRef

  def toDictionary(precision: SimpleFeatureEncoding): Dictionary

  lazy private val map = {
    val builder = scala.collection.mutable.Map.newBuilder[AnyRef, Int]
    builder.sizeHint(length)
    var i = 0
    foreach { value =>
      builder += ((value, i))
      i += 1
    }
    builder.result()
  }

  /**
    * Dictionary encode a value to an int
    *
    * @param value value to encode
    * @return dictionary encoded int
    */
  def index(value: AnyRef): Int = map.getOrElse(value, length)

  def foreach[U](f: AnyRef => U): Unit = iterator.foreach(f)

  def iterator: Iterator[AnyRef] = new Iterator[AnyRef] {
    private var i = 0
    override def hasNext: Boolean = i < ArrowDictionary.this.length
    override def next(): AnyRef = try { lookup(i) } finally { i += 1 }
  }
}

object ArrowDictionary {

  private val r = new java.util.Random()

  private val ids = new AtomicLong(math.abs(r.nextLong()))

  /**
    * Generates a random long usable as a dictionary ID
    *
    * @return random long
    */
  def nextId: Long = ids.getAndSet(math.abs(r.nextLong()))

  /**
    * Create a dictionary based off a sequence of values. Encoding will be smallest that will fit all values.
    *
    * @param values dictionary values
    * @return dictionary
    */
  def create[T <: AnyRef](id: Long, values: Array[T])(implicit ct: ClassTag[T]): ArrowDictionary =
    new ArrowDictionaryArray[T](createEncoding(id, values.length), values, values.length)

  def create[T <: AnyRef](id: Long, values: Array[T], length: Int)(implicit ct: ClassTag[T]): ArrowDictionary =
    new ArrowDictionaryArray[T](createEncoding(id, length), values, length)

  def create(encoding: DictionaryEncoding, values: FieldVector, descriptor: AttributeDescriptor): ArrowDictionary =
    new ArrowDictionaryVector(encoding, values, descriptor)

  /**
    * Holder for dictionary values
    *
    * @param values dictionary values. When encoded, values are replaced with their index in the seq
    * @param encoding dictionary id and int width, id must be unique per arrow file
    */
  class ArrowDictionaryArray[T <: AnyRef](override val encoding: DictionaryEncoding,
                                          values: Array[T],
                                          override val length: Int)
                                          (implicit ct: ClassTag[T]) extends ArrowDictionary {

    override def lookup(i: Int): AnyRef = if (i < length) { values(i) } else { "[other]" }

    override def toDictionary(precision: SimpleFeatureEncoding): Dictionary = {
      import org.locationtech.geomesa.arrow.allocator

      // container for holding our vector
      val container = NullableMapVector.empty("", allocator)

      val name = s"dictionary-$id"

      val (objectType, bindings) = ObjectType.selectType(ct.runtimeClass)
      val writer = ArrowAttributeWriter(name, bindings.+:(objectType), ct.runtimeClass, container, None, Map.empty, precision)

      container.setInitialCapacity(length)
      container.allocateNew()

      val vector = container.getChild(name)
      var i = 0
      while (i < length) {
        writer.apply(i, values(i))
        i += 1
      }
      writer.setValueCount(length)
      vector.getMutator.setValueCount(length)
      new Dictionary(vector, encoding)
    }
  }

  class ArrowDictionaryVector(override val encoding: DictionaryEncoding,
                              vector: FieldVector,
                              descriptor: AttributeDescriptor) extends ArrowDictionary {

    private val featureEncoding =
      if (classOf[Date].isAssignableFrom(descriptor.getType.getBinding) &&
          vector.isInstanceOf[NullableBigIntVector]) {
        SimpleFeatureEncoding(fids = false, EncodingPrecision.Max, EncodingPrecision.Min)
      } else if (classOf[Geometry].isAssignableFrom(descriptor.getType.getBinding) &&
          GeometryFields.precisionFromField(vector.getField) == FloatingPointPrecision.DOUBLE) {
        SimpleFeatureEncoding(fids = false, EncodingPrecision.Max, EncodingPrecision.Min)
      } else {
        SimpleFeatureEncoding.min(fids = false)
      }

    private val reader = ArrowAttributeReader(descriptor, vector, None, featureEncoding)
    private val accessor = vector.getAccessor

    override val length: Int = accessor.getValueCount

    override def lookup(i: Int): AnyRef = if (i < length) { reader.apply(i) } else { "[other]" }

    // TODO verify precision matches vector
    override def toDictionary(precision: SimpleFeatureEncoding): Dictionary = new Dictionary(vector, encoding)
  }

  // use the smallest int type possible to minimize bytes used
  private def createEncoding(id: Long, count: Int): DictionaryEncoding = {
    // we check `MaxValue - 1` to allow for the fallback 'other'
    if (count < Byte.MaxValue - 1) {
      new DictionaryEncoding(id, false, new ArrowType.Int(8, true))
    } else if (count < Short.MaxValue - 1) {
      new DictionaryEncoding(id, false, new ArrowType.Int(16, true))
    } else {
      new DictionaryEncoding(id, false, new ArrowType.Int(32, true))
    }
  }
}
