/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.Date

import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ArrayBuffer

/**
 * Stats are serialized as a byte array where the first byte indicates which type of stat is present.
 * The next four bits contain the size of the serialized information.
 * A SeqStat is serialized the same way, with each individual stat immediately following the previous in the byte array.
 */
object StatSerialization {

  private val Utf8 = Charset.forName("UTF-8")

  // bytes indicating the type of stat
  private val SeqByte: Byte    = '0'
  private val CountByte: Byte  = '1'
  private val MinMaxByte: Byte = '2'
  private val IscByte: Byte    = '3'
  private val EhByte: Byte     = '4'
  private val RhByte: Byte     = '5'

  private val SeqByteArray    = Array(SeqByte)
  private val CountByteArray  = Array(CountByte)
  private val MinMaxByteArray = Array(MinMaxByte)
  private val IscByteArray    = Array(IscByte)
  private val EhByteArray     = Array(EhByte)
  private val RhByteArray     = Array(RhByte)

  /**
    * Uses individual stat pack methods to serialize the stat
    *
    * @param stat the given stat to serialize
    * @return serialized stat
    */
  def pack(stat: Stat, sft: SimpleFeatureType): Array[Byte] = {
    stat match {
      case s: CountStat              => Bytes.concat(CountByteArray, packCount(s))
      case s: MinMax[_]              => Bytes.concat(MinMaxByteArray, packMinMax(s, sft))
      case s: IteratorStackCounter   => Bytes.concat(IscByteArray, packIteratorStackCounter(s))
      case s: EnumeratedHistogram[_] => Bytes.concat(EhByteArray, packEnumeratedHistogram(s, sft))
      case s: RangeHistogram[_]      => Bytes.concat(RhByteArray, packRangeHistogram(s, sft))
      case s: SeqStat                => Bytes.concat(SeqByteArray, packSeqStat(s, sft))
    }
  }

  def unpack(bytes: Array[Byte], sft: SimpleFeatureType): Stat = unpack(bytes, sft, 0, bytes.length)
  /**
    * Deserializes the stat
    *
    * @param bytes the serialized stat
    * @param sft simple feature type that the stat is operating on
    * @return deserialized stat
    */
  def unpack(bytes: Array[Byte], sft: SimpleFeatureType, offset: Int, length: Int): Stat = {
    bytes(offset) match {
      case CountByte  => unpackCount(bytes, offset + 1, length - 1)
      case MinMaxByte => unpackMinMax(bytes, sft, offset + 1, length - 1)
      case IscByte    => unpackIteratorStackCounter(bytes, offset + 1, length - 1)
      case EhByte     => unpackEnumeratedHistogram(bytes, sft, offset + 1, length - 1)
      case RhByte     => unpackRangeHistogram(bytes, sft, offset + 1, length - 1)
      case SeqByte    => unpackSeqStat(bytes, sft, offset + 1, length - 1)
    }
  }

  private def packSeqStat(stat: SeqStat, sft: SimpleFeatureType): Array[Byte] = {
    val seq = stat.stats.map { s =>
      val bytes = pack(s, sft)
      val size = ByteBuffer.allocate(4).putInt(bytes.length).array
      Bytes.concat(size, bytes)
    }
    Bytes.concat(seq: _*)
  }

  private def unpackSeqStat(bytes: Array[Byte], sft: SimpleFeatureType, offset: Int, length: Int): SeqStat = {
    val stats = ArrayBuffer.empty[Stat]
    val bb = ByteBuffer.wrap(bytes)

    var pos = offset
    while (pos < offset + length) {
      bb.position(pos)
      val size = bb.getInt()
      stats.append(unpack(bytes, sft, pos + 4, size))
      pos += (4 + size)
    }

    new SeqStat(stats)
  }

  private def packCount(stat: CountStat): Array[Byte] =
    Bytes.concat(Longs.toByteArray(stat.count), stat.ecql.getBytes(Utf8))

  private def unpackCount(bytes: Array[Byte], offset: Int, length: Int): CountStat = {
    val ecql = new String(bytes, offset + 8, length - 8, Utf8)
    val stat = new CountStat(ecql)
    stat.count = Longs.fromByteArray(bytes.slice(offset, offset + 8))
    stat
  }

  protected [stats] def packMinMax(stat: MinMax[_], sft: SimpleFeatureType): Array[Byte] = {
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    s"${stat.attribute};${stringify(stat.min)};${stringify(stat.max)}".getBytes(Utf8)
  }

  protected [stats] def unpackMinMax(bytes: Array[Byte], sft: SimpleFeatureType, offset: Int, length: Int): MinMax[_] = {
    val split = new String(bytes, offset, length, Utf8).split(";")

    val attribute = split(0).toInt
    val minString = split(1)
    val maxString = split(2)

    val attributeType = sft.getDescriptor(attribute).getType.getBinding
    val destringify = Stat.destringifier(attributeType)
    val min = destringify(minString)
    val max = destringify(maxString)

    val stat = if (attributeType == classOf[String]) {
      new MinMax[String](attribute)
    } else if (attributeType == classOf[Integer]) {
      new MinMax[Integer](attribute)
    } else if (attributeType == classOf[jLong]) {
      new MinMax[jLong](attribute)
    } else if (attributeType == classOf[jFloat]) {
      new MinMax[jFloat](attribute)
    } else if (attributeType == classOf[jDouble]) {
      new MinMax[jDouble](attribute)
    } else if (attributeType == classOf[Date]) {
      new MinMax[Date](attribute)
    } else if (classOf[Geometry].isAssignableFrom(attributeType)) {
      new MinMax[Geometry](attribute)
    } else {
      throw new Exception(s"Cannot unpack MinMax due to invalid type: $attributeType")
    }

    if (min != null) {
      stat.asInstanceOf[MinMax[Any]].minValue = min
      stat.asInstanceOf[MinMax[Any]].maxValue = max
    }

    stat
  }

  protected [stats] def packIteratorStackCounter(stat: IteratorStackCounter): Array[Byte] =
    Longs.toByteArray(stat.count)

  protected [stats] def unpackIteratorStackCounter(bytes: Array[Byte], offset: Int, length: Int): IteratorStackCounter = {
    val stat = new IteratorStackCounter()
    stat.count = Longs.fromByteArray(bytes.slice(offset, offset + length))
    stat
  }

  protected [stats] def packEnumeratedHistogram(stat: EnumeratedHistogram[_], sft: SimpleFeatureType): Array[Byte] = {
    val sb = new StringBuilder(s"${stat.attribute};")
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    val keyValues = stat.histogram.map { case (key, count) => s"${stringify(key)}->$count" }.mkString(",")
    sb.append(keyValues)
    sb.toString().getBytes(Utf8)
  }

  protected[stats] def unpackEnumeratedHistogram(bytes: Array[Byte], sft: SimpleFeatureType,
                                                 offset: Int, length: Int): EnumeratedHistogram[_] = {
    val split = new String(bytes, offset, length, Utf8).split(";")

    val attribute = split(0).toInt
    val attributeType = sft.getDescriptor(attribute).getType.getBinding

    val stat: EnumeratedHistogram[Any] = if (attributeType == classOf[String]) {
      new EnumeratedHistogram[String](attribute).asInstanceOf[EnumeratedHistogram[Any]]
    } else if (attributeType == classOf[Integer]) {
      new EnumeratedHistogram[Integer](attribute).asInstanceOf[EnumeratedHistogram[Any]]
    } else if (attributeType == classOf[jLong]) {
      new EnumeratedHistogram[jLong](attribute).asInstanceOf[EnumeratedHistogram[Any]]
    } else if (attributeType == classOf[jFloat]) {
      new EnumeratedHistogram[jFloat](attribute).asInstanceOf[EnumeratedHistogram[Any]]
    } else if (attributeType == classOf[jDouble]) {
      new EnumeratedHistogram[jDouble](attribute).asInstanceOf[EnumeratedHistogram[Any]]
    } else if (attributeType == classOf[Date]) {
      new EnumeratedHistogram[Date](attribute).asInstanceOf[EnumeratedHistogram[Any]]
    } else if (classOf[Geometry].isAssignableFrom(attributeType)) {
      new EnumeratedHistogram[Geometry](attribute).asInstanceOf[EnumeratedHistogram[Any]]
    } else {
      throw new Exception(s"Cannot unpack EnumeratedHistogram due to invalid type: $attributeType")
    }

    if (split.length > 1) {
      val destringify = Stat.destringifier(attributeType)
      val keyValueStrings = split(1).split(",")
      val keyValues = keyValueStrings.map { keyValuePair =>
        val splitKeyValuePair = keyValuePair.split("->")
        (destringify(splitKeyValuePair(0)), splitKeyValuePair(1).toLong)
      }
      stat.histogram ++= keyValues.asInstanceOf[Array[(Any, Long)]]
    }

    stat
  }

  protected [stats] def packRangeHistogram(stat: RangeHistogram[_], sft: SimpleFeatureType): Array[Byte] = {
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    val sb = new StringBuilder(s"${stat.attribute};${stat.numBins};${stringify(stat.endpoints._1)};${stringify(stat.endpoints._2)};")
    sb.append(stat.bins.counts.mkString(","))
    sb.toString().getBytes(Utf8)
  }

  protected [stats] def unpackRangeHistogram(bytes: Array[Byte], sft: SimpleFeatureType,
                                             offset: Int, length: Int): RangeHistogram[_] = {
    val split = new String(bytes, offset, length, Utf8).split(";")

    val attribute = split(0).toInt
    val numBins = split(1).toInt
    val lowerEndpoint = split(2)
    val upperEndpoint = split(3)
    val counts = split(4).split(",").map(_.toLong)

    val attributeType = sft.getDescriptor(attribute).getType.getBinding
    val destringify = Stat.destringifier(attributeType)
    val bounds = (destringify(lowerEndpoint), destringify(upperEndpoint))

    val stat = if (attributeType == classOf[String]) {
      new RangeHistogram[String](attribute, numBins, bounds.asInstanceOf[(String, String)])
    } else if (attributeType == classOf[Integer]) {
      new RangeHistogram[Integer](attribute, numBins, bounds.asInstanceOf[(Integer, Integer)])
    } else if (attributeType == classOf[jLong]) {
      new RangeHistogram[jLong](attribute, numBins, bounds.asInstanceOf[(jLong, jLong)])
    } else if (attributeType == classOf[jFloat]) {
      new RangeHistogram[jFloat](attribute, numBins, bounds.asInstanceOf[(jFloat, jFloat)])
    } else if (attributeType == classOf[jDouble]) {
      new RangeHistogram[jDouble](attribute, numBins, bounds.asInstanceOf[(jDouble, jDouble)])
    } else if (attributeType == classOf[Date]) {
      new RangeHistogram[Date](attribute, numBins, bounds.asInstanceOf[(Date, Date)])
    } else if (classOf[Geometry].isAssignableFrom(attributeType)) {
      new RangeHistogram[Geometry](attribute, numBins, bounds.asInstanceOf[(Geometry, Geometry)])
    } else {
      throw new Exception(s"Cannot unpack RangeHistogram due to invalid type: $attributeType")
    }

    stat.bins.add(counts)
    stat
  }
}
