/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.util.concurrent.atomic.AtomicLong

import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.locationtech.geomesa.arrow.TypeBindings

/**
  * Holder for dictionary values
  *
  * @param values dictionary values. When encoded, values are replaced with their index in the seq
  * @param encoding dictionary id and int width, id must be unique per arrow file
  */
class ArrowDictionary(val encoding: DictionaryEncoding, values: Array[_ <: AnyRef], val length: Int) {

  def id: Long = encoding.getId

  lazy private val map = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichArray

    val builder = scala.collection.mutable.Map.newBuilder[AnyRef, Int]
    builder.sizeHint(values.length)
    values.foreachIndex { case (value, i) => builder += ((value, i)) }
    builder.result()
  }

  /**
    * Dictionary encode a value to an int
    *
    * @param value value to encode
    * @return dictionary encoded int
    */
  def index(value: AnyRef): Int = map.getOrElse(value, length)

  /**
    * Decode a dictionary int to a value
    *
    * @param i dictionary encoded int
    * @return value
    */
  def lookup(i: Int): AnyRef = {
    if (i < length) {
      values(i)
    } else {
      // catch-all for decoding values that aren't in the dictionary
      // for non-string types, this will evaluate to null
      "[other]"
    }
  }

  def foreach[U](f: AnyRef => U): Unit = {
    var i = 0
    while (i < length) {
      f(values(i))
      i += 1
    }
  }

  def iterator: Iterator[AnyRef] = new Iterator[AnyRef] {
    private var i = 0
    override def hasNext: Boolean = i < ArrowDictionary.this.length
    override def next(): AnyRef = try { values(i) } finally { i += 1 }
  }
}

object ArrowDictionary {

  private val r = new java.util.Random()

  private val ids = new AtomicLong(math.abs(r.nextLong()))

  trait HasArrowDictionary {
    def dictionary: ArrowDictionary
    def dictionaryType: TypeBindings
  }

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
  def create[T <: AnyRef](id: Long, values: Array[T]): ArrowDictionary =
    new ArrowDictionary(createEncoding(id, values.length), values, values.length)

  def create[T <: AnyRef](id: Long, values: Array[T], length: Int): ArrowDictionary =
    new ArrowDictionary(createEncoding(id, length), values, length)

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
