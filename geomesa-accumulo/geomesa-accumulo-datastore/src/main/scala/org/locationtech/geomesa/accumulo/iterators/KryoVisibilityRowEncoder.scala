/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.esotericsoftware.kryo.io.Output
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.user.RowEncodingIterator
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.serialization.CacheKeyGenerator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Assumes cq are byte-encoded attribute number
  */
class KryoVisibilityRowEncoder extends RowEncodingIterator {

  private var sft: SimpleFeatureType = null
  private var nullBytes: Array[Array[Byte]] = null
  private var offsets: Array[Int] = null

  private val output: Output = new Output(128, -1)

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {

    IteratorClassLoader.initClassLoader(getClass)

    super.init(source, options, env)

    sft = SimpleFeatureTypes.createType("", options.get(KryoVisibilityRowEncoder.SftOpt))
    if (offsets == null || offsets.length != sft.getAttributeCount) {
      offsets = Array.ofDim[Int](sft.getAttributeCount)
    }
    val cacheKey = CacheKeyGenerator.cacheKeyForSFT(sft)
    nullBytes = KryoFeatureSerializer.getWriters(cacheKey, sft).map { writer =>
      output.clear()
      writer(output, null)
      output.toBytes
    }
  }

  override def rowEncoder(keys: java.util.List[Key], values: java.util.List[Value]): Value = {
    if (values.size() == 1) {
      return values.get(0)
    }

    val allValues = Array.ofDim[Array[Byte]](sft.getAttributeCount)
    var i = 0
    while (i < keys.size) {
      val cq = keys.get(i).getColumnQualifier
      val comma = cq.find(",")
      val indices = if (comma == -1) cq.getBytes.map(_.toInt) else cq.getBytes.drop(comma + 1).map(_.toInt)
      val bytes = values.get(i).get

      val offsetStart = readOffsets(bytes)

      // set the non-null values
      var j = 0
      while (j < offsets.length - 1) {
        val endIndex = offsets.indexWhere(_ != -1, j)
        val end = if (endIndex == -1) offsetStart else offsets(endIndex)
        val length = end - offsets(j)
        if (allValues(j) == null || KryoVisibilityRowEncoder.notNull(bytes, offsets(j), length, nullBytes(j))) {
          val values = Array.ofDim[Byte](length)
          System.arraycopy(bytes, offsets(j), values, 0, length)
          allValues(j) = values
        }
        j += 1
      }

      i += 1
    }

    i = 0
    while (i < allValues.length) {
      if (allValues(i) == null) {
        allValues(i) = nullBytes(i)
      }
      i += 1
    }

    // TODO if we don't have a geometry, skip the record?
    KryoVisibilityRowEncoder.encode(allValues, output, offsets)
  }

  /**
    * Reads offsets in the 'offsets' array and returns the start of the offset block
    *
    * @param bytes kryo feature bytes
    * @return
    */
  private def readOffsets(bytes: Array[Byte]): Int = {
    val input = KryoFeatureSerializer.getInput(bytes)
    // reset our offsets
    input.setPosition(1) // skip version
    val offsetStart = input.readInt()
    input.setPosition(offsetStart) // set to offsets start
    var j = 0
    while (j < offsets.length) {
      offsets(j) = if (input.position < input.limit) input.readInt(true) else -1
      j += 1
    }
    offsetStart
  }

  override def rowDecoder(rowKey: Key, rowValue: Value): java.util.SortedMap[Key, Value] =
    throw new NotImplementedError("")

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val iterator = new KryoVisibilityRowEncoder
    if (sourceIter != null) {
      iterator.sourceIter = sourceIter.deepCopy(env)
    }
    iterator.sft = sft
    iterator.offsets = Array.ofDim[Int](sft.getAttributeCount)
    iterator.nullBytes = nullBytes
    iterator
  }
}

object KryoVisibilityRowEncoder {

  val SftOpt   = "sft"
  val TableOpt = "table"

  val DefaultPriority = 21 // needs to be first thing that runs after the versioning iterator at 20

  def configure(sft: SimpleFeatureType, table: GeoMesaTable, priority: Int = DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "feature-merge-iter", classOf[KryoVisibilityRowEncoder])
    is.addOption(SftOpt, SimpleFeatureTypes.encodeType(sft, includeUserData = true)) // need user data for id calc
    is.addOption(TableOpt, table.getClass.getSimpleName)
    is
  }

  private def notNull(array: Array[Byte], offset: Int, length: Int, nullBytes: Array[Byte]): Boolean = {
    if (nullBytes.length != length) {
      return true
    }

    var i = 0
    while (i < length) {
      if (array(offset + i) != nullBytes(i)) {
        return false
      }
      i += 1
    }
    true
  }

  private def encode(values: Array[Array[Byte]], output: Output, offsets: Array[Int]): Value = {
    output.clear()
    output.writeInt(KryoFeatureSerializer.VERSION, true)
    output.setPosition(5) // leave 4 bytes to write the offsets
    // note: we don't write ID - tables are assumed to be using serialization without IDs
    // write attributes and keep track off offset into byte array
    var i = 0
    while (i < values.length) {
      offsets(i) = output.position()
      output.write(values(i))
      i += 1
    }
    // write the offsets - variable width
    i = 0
    val offsetStart = output.position()
    while (i < values.length) {
      output.writeInt(offsets(i), true)
      i += 1
    }
    // got back and write the start position for the offsets
    val total = output.position()
    output.setPosition(1)
    output.writeInt(offsetStart)
    output.setPosition(total) // set back to the end so that we get all the bytes

    new Value(output.toBytes)
  }
}
