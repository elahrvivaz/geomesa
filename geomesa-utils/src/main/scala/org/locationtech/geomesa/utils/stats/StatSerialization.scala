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
import java.util.Date

import com.google.common.primitives.Bytes
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.stats.MinMaxHelper._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ArrayBuffer

/**
 * Stats are serialized as a byte array where the first byte indicates which type of stat is present.
 * The next four bits contain the size of the serialized information.
 * A SeqStat is serialized the same way, with each individual stat immediately following the previous in the byte array.
 */
object StatSerialization {

  // bytes indicating the type of stat
  val MINMAX_BYTE: Byte = '0'
  val ISC_BYTE: Byte = '1'
  val EH_BYTE: Byte = '2'
  val RH_BYTE: Byte = '3'

  /**
   * Fully serializes a stat by formatting the byte array with the "kind" byte
   *
   * @param kind byte indicating the type of stat
   * @param bytes serialized stat
   * @return fully serialized stat
   */
  private def serializeStat(kind: Byte, bytes: Array[Byte]): Array[Byte] = {
    val size = ByteBuffer.allocate(4).putInt(bytes.length).array
    Bytes.concat(Array(kind), size, bytes)
  }

  protected [stats] def packMinMax(stat: MinMax[_], sft: SimpleFeatureType): Array[Byte] = {
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    val asString = s"${stat.attribute};${stringify(stat.min)};${stringify(stat.max)}"
    serializeStat(MINMAX_BYTE, asString.getBytes("UTF-8"))
  }

  protected [stats] def unpackMinMax(bytes: Array[Byte], sft: SimpleFeatureType): MinMax[_] = {
    val split = new String(bytes, "UTF-8").split(";")

    val attribute = split(0).toInt
    val minString = split(1)
    val maxString = split(2)

    val attributeType = sft.getDescriptor(attribute).getType.getBinding
    val destringify = Stat.destringifier(attributeType)
    val min = destringify(minString)
    val max = destringify(maxString)

    if (attributeType == classOf[String]) {
      val stat = new MinMax[String](attribute)
      if (min != null) {
        stat.updateMin(min.asInstanceOf[String])
        stat.updateMax(max.asInstanceOf[String])
      }
      stat
    } else if (attributeType == classOf[Integer]) {
      val stat = new MinMax[Integer](attribute)
      if (min != null) {
        stat.updateMin(min.asInstanceOf[Integer])
        stat.updateMax(max.asInstanceOf[Integer])
      }
      stat
    } else if (attributeType == classOf[jLong]) {
      val stat = new MinMax[jLong](attribute)
      if (min != null) {
        stat.updateMin(min.asInstanceOf[jLong])
        stat.updateMax(max.asInstanceOf[jLong])
      }
      stat
    } else if (attributeType == classOf[jFloat]) {
      val stat = new MinMax[jFloat](attribute)
      if (min != null) {
        stat.updateMin(min.asInstanceOf[jFloat])
        stat.updateMax(max.asInstanceOf[jFloat])
      }
      stat
    } else if (attributeType == classOf[jDouble]) {
      val stat = new MinMax[jDouble](attribute)
      if (min != null) {
        stat.updateMin(min.asInstanceOf[jDouble])
        stat.updateMax(max.asInstanceOf[jDouble])
      }
      stat
    } else if (attributeType == classOf[Date]) {
      val stat = new MinMax[Date](attribute)
      if (min != null) {
        stat.updateMin(min.asInstanceOf[Date])
        stat.updateMax(max.asInstanceOf[Date])
      }
      stat
    } else if (classOf[Geometry].isAssignableFrom(attributeType)) {
      val stat = new MinMax[Geometry](attribute)
      if (min != null) {
        stat.updateMin(min.asInstanceOf[Geometry])
        stat.updateMax(max.asInstanceOf[Geometry])
      }
      stat
    } else {
      throw new Exception(s"Cannot unpack MinMax due to invalid type: $attributeType")
    }
  }

  protected [stats] def packISC(stat: IteratorStackCounter): Array[Byte] = {
    serializeStat(ISC_BYTE, s"${stat.count}".getBytes("UTF-8"))
  }

  protected [stats] def unpackIteratorStackCounter(bytes: Array[Byte]): IteratorStackCounter = {
    val stat = new IteratorStackCounter()
    stat.count = jLong.parseLong(new String(bytes, "UTF-8"))
    stat
  }

  protected [stats] def packEnumeratedHistogram(stat: EnumeratedHistogram[_], sft: SimpleFeatureType): Array[Byte] = {
    val sb = new StringBuilder(s"${stat.attribute};")
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    val keyValues = stat.histogram.map { case (key, count) => s"${stringify(key)}->$count" }.mkString(",")
    sb.append(keyValues)
    serializeStat(EH_BYTE, sb.toString().getBytes("UTF-8"))
  }

  protected[stats] def unpackEnumeratedHistogram(bytes: Array[Byte], sft: SimpleFeatureType): EnumeratedHistogram[_] = {
    val split = new String(bytes).split(";")

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
    serializeStat(RH_BYTE, sb.toString().getBytes("UTF-8"))
  }

  protected [stats] def unpackRangeHistogram(bytes: Array[Byte], sft: SimpleFeatureType): RangeHistogram[_] = {
    val split = new String(bytes, "UTF-8").split(";")

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

  /**
   * Uses individual stat pack methods to serialize the stat
   *
   * @param stat the given stat to serialize
   * @return serialized stat
   */
  def pack(stat: Stat, sft: SimpleFeatureType): Array[Byte] = {
    stat match {
      case mm: MinMax[_]              => packMinMax(mm, sft)
      case isc: IteratorStackCounter  => packISC(isc)
      case eh: EnumeratedHistogram[_] => packEnumeratedHistogram(eh, sft)
      case rh: RangeHistogram[_]      => packRangeHistogram(rh, sft)
      case seq: SeqStat               => Bytes.concat(seq.stats.map(pack(_, sft)): _*)
    }
  }

  /**
   * Deserializes the stat
   *
   * @param bytes the serialized stat
   * @param sft simple feature type that the stat is operating on
   * @return deserialized stat
   */
  def unpack(bytes: Array[Byte], sft: SimpleFeatureType): Stat = {
    val returnStats = ArrayBuffer.empty[Stat]
    val bb = ByteBuffer.wrap(bytes)

    while (bb.hasRemaining) {
      val statType = bb.get()
      val statBytes = Array.ofDim[Byte](bb.getInt()) // stat size
      bb.get(statBytes)

      val stat = statType match {
        case MINMAX_BYTE => unpackMinMax(statBytes, sft)
        case ISC_BYTE    => unpackIteratorStackCounter(statBytes)
        case EH_BYTE     => unpackEnumeratedHistogram(statBytes, sft)
        case RH_BYTE     => unpackRangeHistogram(statBytes, sft)
      }

      returnStats.append(stat)
    }

    if (returnStats.length == 1) {
      returnStats.head
    } else {
      new SeqStat(returnStats)
    }
  }
}
