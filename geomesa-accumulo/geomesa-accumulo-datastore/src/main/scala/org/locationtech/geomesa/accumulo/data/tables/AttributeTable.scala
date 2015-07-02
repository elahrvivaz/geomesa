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
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
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
          getMutations(toWrite, attributesToIdx, prefixBytes, timeBytes ++ idBytes, delete)
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
        val bytes = attribute.getBytes(UTF8)
        val parts = Seq(prefix, indexBytes, bytes, NULLBYTE, suffix)
        val buffer = Array.ofDim[Byte](parts.map(_.length).sum)
        var i = 0
        parts.foreach { part =>
          System.arraycopy(part, 0, buffer, i, part.length)
          i += part.length
        }
        new Mutation(buffer)
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
    sft.getTableSharingPrefix.getBytes(UTF8) ++ indexToBytes(i)

  /**
   * Gets a full row (minus the feature ID) for the given value, needed for creating query ranges.
   * Less optimized version of logic from getMutations.
   */
  def getRow(sft: SimpleFeatureType, i: Int, value: Any, time: Option[Long]): Option[Array[Byte]] = {
    val prefix = getRowPrefix(sft, i)
    val suffix = time.map(NULLBYTE ++ timeToBytes(_)).getOrElse(NULLBYTE)
    encode(value, sft.getDescriptor(i)).headOption.map(prefix ++ _.getBytes(UTF8) ++ suffix)
  }

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
