/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.data.tables

import java.nio.charset.Charset
import java.util.{Collection => JCollection, Date, Locale}

import com.google.common.collect.ImmutableSortedSet
import com.google.common.primitives.Bytes
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Range => AccRange}
import org.apache.hadoop.io.Text
import org.calrissian.mango.types.{LexiTypeEncoders, SimpleTypeEncoders, TypeEncoder}
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Contains logic for converting between accumulo and geotools for the attribute index
 */
object AttributeTable extends GeoMesaTable with Logging {

  private val UTF8 = Charset.forName("UTF-8")
  private val NULLBYTE = "\u0000".getBytes(UTF8)

  private val typeRegistry   = LexiTypeEncoders.LEXI_TYPES
  private val simpleEncoders = SimpleTypeEncoders.SIMPLE_TYPES.getAllEncoders
  private val dateFormat     = ISODateTimeFormat.dateTime()

  private type TryEncoder = Try[(TypeEncoder[Any, String], TypeEncoder[_, String])]

  override def supports(sft: SimpleFeatureType) =
    sft.getSchemaVersion > 5 && SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).nonEmpty

  override val suffix: String = "attr_idx"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = mutator(sft, delete = false)

  override def remover(sft: SimpleFeatureType): FeatureToMutations = mutator(sft, delete = true)

  private def mutator(sft: SimpleFeatureType, delete: Boolean): FeatureToMutations = {
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)
    val indexesOfIndexedAttributes = indexedAttributes.map(a => sft.indexOf(a.getName))
    val attributesToIdx = indexedAttributes.zip(indexesOfIndexedAttributes)
    val prefixBytes = sft.getTableSharingPrefix.getBytes(UTF8)

    sft.getDtgIndex match {
      case None =>
        (toWrite: FeatureToWrite) => {
          val idBytes = toWrite.feature.getID.getBytes(UTF8)
          getMutations(toWrite, attributesToIdx, prefixBytes, idBytes, delete)
        }
      case Some(dtgIndex) =>
        (toWrite: FeatureToWrite) => {
          val dtg = toWrite.feature.getAttribute(dtgIndex).asInstanceOf[Date]
          val time = if (dtg == null) System.currentTimeMillis() else dtg.getTime
          val timeBytes = timeToBytes(time)
          val idBytes = toWrite.feature.getID.getBytes(UTF8)
          getMutations(toWrite, attributesToIdx, prefixBytes, Bytes.concat(timeBytes, idBytes), delete)
        }
    }
  }

  /**
   * Rows in the attribute table have the following layout:
   *
   * - 1 byte identifying the sft (OPTIONAL - only if table is shared)
   * - 2 bytes storing the index of the attribute in the sft
   * - n bytes storing the lexicoded attribute value
   * - NULLBYTE as a separator
   * - 12 bytes storing the dtg of the feature (OPTIONAL - only if the sft has a dtg field)
   * - n bytes storing the feature ID
   */
  private def getMutations(toWrite: FeatureToWrite,
                           indexedAttributes: Seq[(AttributeDescriptor, Int)],
                           prefix: Array[Byte],
                           suffix: Array[Byte],
                           delete: Boolean): Seq[Mutation] = {
    indexedAttributes.flatMap { case (descriptor, idx) =>
      val indexBytes = indexToBytes(idx)
      val attributes = encode(toWrite.feature.getAttribute(idx), descriptor)
      val mutations = attributes.map { attribute =>
        new Mutation(Bytes.concat(prefix, indexBytes, attribute.getBytes(UTF8), NULLBYTE, suffix))
      }
      if (delete) {
        mutations.foreach(_.putDelete(EMPTY_TEXT, EMPTY_TEXT, toWrite.columnVisibility))
      } else {
        val value = descriptor.getIndexCoverage() match {
          case IndexCoverage.FULL => toWrite.dataValue
          case IndexCoverage.JOIN => toWrite.indexValue
        }
        mutations.foreach(_.put(EMPTY_TEXT, EMPTY_TEXT, toWrite.columnVisibility, value))
      }
      mutations
    }
  }

  // store 2 bytes for the index of the attribute in the sft - this allows up to 32k attributes in the sft.
  def indexToBytes(i: Int) = Array((i << 8).asInstanceOf[Byte], i.asInstanceOf[Byte])

  // store the first 12 hex chars of the time - that is roughly down to the minute interval
  def timeToBytes(t: Long) = typeRegistry.encode(t).substring(0, 12).getBytes(UTF8)

  /**
   * Gets a prefix for an attribute row - this includes the sft and the attribute index only
   */
  def getRowPrefix(sft: SimpleFeatureType, i: Int): Array[Byte] =
    Bytes.concat(sft.getTableSharingPrefix.getBytes(UTF8), indexToBytes(i))

  // equals range
  def equals(sft: SimpleFeatureType, i: Int, value: Any, times: Option[(Long, Long)]): AccRange = {
    times match {
      case None => AccRange.prefix(row(sft, i, value, following = false, None))
      case Some((sTime, eTime)) =>
        val start = row(sft, i, value, following = false, Some(sTime))
        val end = row(sft, i, value, following = false, Some(eTime))
        new AccRange(start, true, end, true)
    }
  }

  // less than range
  def lt(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lowerBound(sft, i), true, upper(sft, i, value, inclusive = false, time), false)

  // less than or equal to range
  def lte(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lowerBound(sft, i), true, upper(sft, i, value, inclusive = true, time), false)

  // greater than range
  def gt(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lower(sft, i, value, inclusive = false, time), true, upperBound(sft, i), false)

  // greater than or equal to range
  def gte(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): AccRange =
    new AccRange(lower(sft, i, value, inclusive = true, time), true, upperBound(sft, i), false)

  // between range
  def between(sft: SimpleFeatureType, i: Int, values: (Any, Any), inclusive: Boolean, times: Option[(Long, Long)]): AccRange = {
    val start = lower(sft, i, values._1, inclusive, times.map(_._1))
    val end = upper(sft, i, values._2, inclusive, times.map(_._2))
    new AccRange(start, true, end, false)
  }

  // prefix range - doesn't account for times
  def prefix(sft: SimpleFeatureType, i: Int, value: Any): AccRange = {
    val prefix = getRowPrefix(sft, i)
    val encoded = encode(getTypedValue(sft, i, value), sft.getDescriptor(i)).headOption
    val encodedBytes = encoded.map(_.getBytes(UTF8)).getOrElse(Array.empty)
    AccRange.prefix(new Text(Bytes.concat(prefix, encodedBytes)))
  }

  // all values for this attribute
  def all(sft: SimpleFeatureType, i: Int): AccRange =
    new AccRange(lowerBound(sft, i), true, upperBound(sft, i), false)

  // gets a lower bound for a range (inclusive)
  private def lower(sft: SimpleFeatureType, i: Int, value: Any, inclusive: Boolean, time: Option[Long]): Text =
    row(sft, i, value, !inclusive, time)

  // gets an upper bound for a range (exclusive)
  private def upper(sft: SimpleFeatureType, i: Int, value: Any, inclusive: Boolean, time: Option[Long]): Text =
    row(sft, i, value, inclusive, time)

  // gets a row for use in a range
  private def row(sft: SimpleFeatureType, i: Int, value: Any, following: Boolean, time: Option[Long]): Text = {
    val prefix = getRowPrefix(sft, i)
    val timeBytes = time.map(timeToBytes).getOrElse(Array.empty)
    val encoded = encode(getTypedValue(sft, i, value), sft.getDescriptor(i)).headOption
    val encodedBytes = encoded.map(_.getBytes(UTF8)).getOrElse(Array.empty)
    if (following) {
      val row = AccRange.followingPrefix(new Text(Bytes.concat(prefix, encodedBytes)))
      new Text(Bytes.concat(row.getBytes, NULLBYTE, timeBytes))
    } else {
      new Text(Bytes.concat(prefix, encodedBytes, NULLBYTE, timeBytes))
    }
  }

  // lower bound for all values of the attribute, inclusive
  private def lowerBound(sft: SimpleFeatureType, i: Int): Text = new Text(AttributeTable.getRowPrefix(sft, i))

  // upper bound for all values of the attribute, exclusive
  private def upperBound(sft: SimpleFeatureType, i: Int): Text =
    AccRange.followingPrefix(new Text(AttributeTable.getRowPrefix(sft, i)))

  /**
   * Decodes an attribute value out of row string
   */
  def decodeRow(sft: SimpleFeatureType, i: Int, row: Array[Byte]): Try[Any] = Try {
    val from = if (sft.isTableSharing) 3 else 2
    val encodedValue = row.slice(from, row.indexOf(NULLBYTE(0), from + 1))
    decode(new String(encodedValue, UTF8), sft.getDescriptor(i))
  }

  /**
   * Gets the feature ID from the row key
   */
  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = {
    val from = if (sft.isTableSharing) 4 else 3
    sft.getDtgField match {
      case None    => (row) => new String(row.drop(row.indexOf(NULLBYTE(0), from) + 1), UTF8)
      case Some(_) => (row) => new String(row.drop(row.indexOf(NULLBYTE(0), from) + 13), UTF8)
    }
  }

  /**
   * Lexicographically encode the value. Collections will return multiple rows, one for each entry.
   *
   * @param value
   * @param descriptor
   * @return
   */
  def encode(value: Any, descriptor: AttributeDescriptor): Seq[String] = {
    if (value == null) {
      Seq.empty
    } else if (descriptor.isCollection) {
      // encode each value into a separate row
      value.asInstanceOf[JCollection[_]].toSeq.flatMap(Option(_).map(typeEncode).filterNot(_.isEmpty))
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Seq.empty
    } else {
      val encoded = typeEncode(value)
      if (encoded.isEmpty) Seq.empty else Seq(encoded)
    }
  }

  private def typeEncode(value: Any): String = Try(typeRegistry.encode(value)).getOrElse(value.toString)

  /**
   * Decode an encoded value. Note that for collection types, only a single entry of the collection
   * will be decoded - this is because the collection entries have been broken up into multiple rows.
   *
   * @param encoded
   * @param descriptor
   * @return
   */
  def decode(encoded: String, descriptor: AttributeDescriptor): Any = {
    if (descriptor.isCollection) {
      // get the alias from the type of values in the collection
      val alias = descriptor.getCollectionType().map(_.getSimpleName.toLowerCase(Locale.US)).head
      Seq(typeRegistry.decode(alias, encoded)).asJava
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Map.empty.asJava
    } else {
      val alias = descriptor.getType.getBinding.getSimpleName.toLowerCase(Locale.US)
      typeRegistry.decode(alias, encoded)
    }
  }

  /**
   * Tries to convert a value from one class to another. When querying attributes, the query
   * literal has to match the class of the attribute for lexicoding to work.
   *
   * @param value
   * @param current
   * @param desired
   * @return
   */
  def convertType(value: Any, current: Class[_], desired: Class[_]): Any = {
    val result =
      if (current == desired) {
        Success(value)
      } else if (desired == classOf[Date] && current == classOf[String]) {
        // try to parse the string as a date - right now we support just ISO format
        Try(dateFormat.parseDateTime(value.asInstanceOf[String]).toDate)
      } else {
        // cheap way to convert between basic classes (string, int, double, etc) - encode the value
        // to a string and then decode to the desired class
        val encoderOpt = simpleEncoders.find(_.resolves() == current).map(_.asInstanceOf[TypeEncoder[Any, String]])
        val decoderOpt = simpleEncoders.find(_.resolves() == desired)
        (encoderOpt, decoderOpt) match {
          case (Some(e), Some(d)) => Try(d.decode(e.encode(value)))
          case _ => Failure(new RuntimeException("No matching encoder/decoder"))
        }
      }

    result match {
      case Success(converted) => converted
      case Failure(e) =>
        logger.warn(s"Error converting type for '$value' from ${current.getSimpleName} to " +
          s"${desired.getSimpleName}: ${e.toString}")
        value
    }
  }

  /**
   * Gets a value that can used to compute a range for an attribute query.
   * The attribute index encodes the type of the attribute as part of the row. This checks for
   * query literals that don't match the expected type and tries to convert them.
   */
  def getTypedValue(sft: SimpleFeatureType, prop: Int, value: Any): Any = {
    val descriptor = sft.getDescriptor(prop)
    // the class type as defined in the SFT
    val expectedBinding = descriptor.getType.getBinding
    // the class type of the literal pulled from the query
    val actualBinding = value.getClass
    if (expectedBinding == actualBinding) {
      value
    } else if (descriptor.isCollection) {
      // we need to encode with the collection type
      descriptor.getCollectionType() match {
        case Some(collectionType) if collectionType == actualBinding => Seq(value).asJava
        case Some(collectionType) if collectionType != actualBinding =>
          Seq(AttributeTable.convertType(value, actualBinding, collectionType)).asJava
      }
    } else if (descriptor.isMap) {
      // TODO GEOMESA-454 - support querying against map attributes
      Map.empty.asJava
    } else {
      // type mismatch, encoding won't work b/c value is wrong class
      // try to convert to the appropriate class
      AttributeTable.convertType(value, actualBinding, expectedBinding)
    }
  }

  override def configureTable(featureType: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    val indexedAttrs = SimpleFeatureTypes.getSecondaryIndexedAttributes(featureType)
    if (indexedAttrs.nonEmpty) {
      val indices = indexedAttrs.map(d => featureType.indexOf(d.getLocalName))
      val splits = indices.map(i => new Text(getRowPrefix(featureType, i)))
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splits.toArray))
    }
  }
}

case class AttributeIndexRow(attributeName: String, attributeValue: Any)
