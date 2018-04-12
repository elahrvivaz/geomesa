/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Date

import com.vividsolutions.jts.geom.Envelope
import org.apache.kudu.client.RowResult
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.arrow.io.{DeltaWriter, DictionaryBuildingWriter}
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.InMemoryQueryRunner._
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.security.{SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap, SimpleFeatureOrdering, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.{Stat, TopK}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Expression, PropertyName}

sealed trait KuduResultAdapter {
  def sft: SimpleFeatureType
  def columns: Seq[String]
  def auths: Seq[Array[Byte]]
  def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature]
}

object KuduResultAdapter {

  import org.locationtech.geomesa.filter.filterToString
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  /**
    * Turns scan results into simple features
    *
    * @param sft simple feature type
    * @param ecql filter to apply
    * @param hints query hints
    * @return (columns required, adapter for rows)
    */
  def apply(sft: SimpleFeatureType, ecql: Option[Filter], hints: Hints, auths: Seq[Array[Byte]]): KuduResultAdapter = {
    if (hints.isBinQuery) {
      val trackId = Option(hints.getBinTrackIdField)
      val geom = hints.getBinGeomField
      val dtg = hints.getBinDtgField
      new BinAdapter(sft, ecql, auths, trackId, geom, dtg, hints.getBinLabelField, hints.isBinSorting)
    } else if (hints.isArrowQuery) {
      new ArrowAdapter(sft, ecql, auths, hints)
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      new DensityAdapter(sft, ecql, auths, envelope, width, height, hints.getDensityWeight)
    } else if (hints.isStatsQuery) {
      new StatsAdapter(sft, ecql, auths, hints.getTransform, hints.getStatsQuery, hints.isStatsEncode || hints.isSkipReduce)
    } else {
//      val sort = hints.getSortFields.map(SimpleFeatureOrdering(sft, _))
//      val sort = hints.getSortFields.map(SimpleFeatureOrdering(tsft, _))
      (hints.getTransform, ecql) match {
        case (None, None)                   => new DirectAdapter(sft, auths)
        case (None, Some(f))                => new FilterAdapter(sft, auths, f)
        case (Some((tdefs, tsft)), None)    => new TransformAdapter(sft, auths, tsft, tdefs)
        case (Some((tdefs, tsft)), Some(f)) => new FilterTransformAdapter(sft, auths, f, tsft, tdefs)
      }
    }
  }

  def serialize(adapter: KuduResultAdapter): Array[Byte] = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    var bb = ByteBuffer.allocate(1024)

    def putString(s: String): Unit = putArray(s.getBytes(StandardCharsets.UTF_8))

    def putArray(bytes: Array[Byte]): Unit = {
      bb = bb.ensureRemaining(bytes.length + 4)
      bb.putBytes(bytes)
    }

    def putInt(int: Int): Unit = {
      bb = bb.ensureRemaining(4)
      bb.putInt(int)
    }

    putString(adapter.sft.getTypeName)
    putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    putInt(adapter.auths.length)
    adapter.auths.foreach(putArray)

    val (ecql, transform) = adapter match {
      case EmptyAdapter              => (None, None)
      case _: DirectAdapter          => (None, None)
      case a: FilterAdapter          => (Some(a.ecql), None)
      case a: TransformAdapter       => (None, Some((a.tsft, a.tdefs)))
      case a: FilterTransformAdapter => (Some(a.ecql), Some((a.tsft, a.tdefs)))
    }

    putString(ecql.map(ECQL.toCQL).getOrElse(""))
    putString(transform.map(t => SimpleFeatureTypes.encodeType(t._1)).getOrElse(""))
    putString(transform.map(_._2).getOrElse(""))

    bb.toArray
  }

  def deserialize(bytes: Array[Byte]): KuduResultAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val bb = ByteBuffer.wrap(bytes)

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)

    if (sft.getTypeName.length == 0) { EmptyAdapter } else {
      val auths = (0 until bb.getInt).map(_ => bb.getBytes)
      val ecql = Some(bb.getString).filter(_.length > 0).map(FastFilterFactory.toFilter(sft, _))
      val transform = Some(bb.getString).filter(_.length > 0).map { spec =>
        (SimpleFeatureTypes.createType(sft.getTypeName, spec), bb.getString)
      }

      (ecql, transform) match {
        case (None, None)                   => new DirectAdapter(sft, auths)
        case (Some(e), None)                => new FilterAdapter(sft, auths, e)
        case (None, Some((tsft, tdefs)))    => new TransformAdapter(sft, auths, tsft, tdefs)
        case (Some(e), Some((tsft, tdefs))) => new FilterTransformAdapter(sft, auths, e, tsft, tdefs)
      }
    }
  }

  private def isVisible(auths: Seq[Array[Byte]], feature: SimpleFeature): Boolean = {
    val vis = SecurityUtils.getVisibility(feature)
    vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)
  }

  private def equals(one: Seq[Array[Byte]], two: Seq[Array[Byte]]): Boolean =
    one.lengthCompare(two.length) == 0 && one.zip(two).forall { case (o, t) => java.util.Arrays.equals(o, t) }

  object EmptyAdapter extends KuduResultAdapter {
    override val sft: SimpleFeatureType = SimpleFeatureTypes.createType("", "")
    override val columns: Seq[String] = Seq.empty
    override val auths: Seq[Array[Byte]] = Seq.empty
    override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = CloseableIterator.empty
  }

  class DirectAdapter(override val sft: SimpleFeatureType,
                      override val auths: Seq[Array[Byte]]) extends KuduResultAdapter {

    private val schema = KuduSimpleFeatureSchema(sft)
    private val deserializer = schema.deserializer
    private val feature = new ScalaSimpleFeature(sft, "")

    override val columns: Seq[String] =
      Seq(FeatureIdAdapter.name, VisibilityAdapter.name) ++ schema.schema.map(_.getName)

    override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
      results.flatMap { row =>
        feature.setId(FeatureIdAdapter.readFromRow(row))
        deserializer.deserialize(row, feature)
        SecurityUtils.setFeatureVisibility(feature, VisibilityAdapter.readFromRow(row))
        if (isVisible(auths, feature)) {
          Iterator.single(feature)
        } else {
          CloseableIterator.empty
        }
      }
    }

    override def equals(other: Any): Boolean = other match {
      case that: DirectAdapter => sft == that.sft && KuduResultAdapter.equals(auths, that.auths)
      case _ => false
    }

    override def hashCode(): Int = Seq(sft, auths).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

    override def toString: String =
      s"DirectAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
  }

  class FilterAdapter(override val sft: SimpleFeatureType,
                      override val auths: Seq[Array[Byte]],
                      val ecql: Filter) extends KuduResultAdapter {

    private val schema = KuduSimpleFeatureSchema(sft)
    private val deserializer = schema.deserializer
    private val feature = new ScalaSimpleFeature(sft, "")

    override val columns: Seq[String] =
      Seq(FeatureIdAdapter.name, VisibilityAdapter.name) ++ schema.schema.map(_.getName)

    override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
      results.flatMap { row =>
        feature.setId(FeatureIdAdapter.readFromRow(row))
        deserializer.deserialize(row, feature)
        SecurityUtils.setFeatureVisibility(feature, VisibilityAdapter.readFromRow(row))
        if (isVisible(auths, feature) && ecql.evaluate(feature)) {
          Iterator.single(feature)
        } else {
          CloseableIterator.empty
        }
      }
    }

    override def equals(other: Any): Boolean = other match {
      case that: FilterAdapter =>
        sft == that.sft && KuduResultAdapter.equals(auths, that.auths) &&
            filterToString(ecql) == filterToString(that.ecql)
      case _ => false
    }

    override def hashCode(): Int = Seq(sft, auths, ecql).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

    override def toString: String =
      s"FilterAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"filter=${filterToString(ecql)}, auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
  }

  class TransformAdapter(override val sft: SimpleFeatureType,
                         override val auths: Seq[Array[Byte]],
                         val tsft: SimpleFeatureType,
                         val tdefs: String) extends KuduResultAdapter {

    import scala.collection.JavaConverters._

    // determine all the attributes that we need to be able to evaluate the transform
    private val attributes = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
      case p: PropertyName => Seq(p.getPropertyName)
      case e: Expression   => DataUtilities.attributeNames(e, sft)
    }.distinct

    private val subType = DataUtilities.createSubType(sft, attributes.toArray)
    subType.getUserData.putAll(sft.getUserData)

    private val schema = KuduSimpleFeatureSchema(sft)
    private val deserializer = schema.deserializer(subType)
    private val feature = new ScalaSimpleFeature(subType, "")
    private val transformFeature = TransformSimpleFeature(subType, tsft, tdefs)
    transformFeature.setFeature(feature)

    override val columns: Seq[String] =
      Seq(FeatureIdAdapter.name, VisibilityAdapter.name) ++ schema.schema(attributes).map(_.getName)

    override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
      results.flatMap { row =>
        feature.setId(FeatureIdAdapter.readFromRow(row))
        deserializer.deserialize(row, feature)
        SecurityUtils.setFeatureVisibility(feature, VisibilityAdapter.readFromRow(row))
        if (isVisible(auths, feature)) {
          Iterator.single(transformFeature)
        } else {
          CloseableIterator.empty
        }
      }
    }

    override def equals(other: Any): Boolean = other match {
      case that: TransformAdapter =>
        sft == that.sft && KuduResultAdapter.equals(auths, that.auths) && tdefs == that.tdefs
      case _ => false
    }

    override def hashCode(): Int = Seq(sft, auths, tdefs).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

    override def toString: String =
      s"TransformAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"transform=$tdefs, auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
  }

  class FilterTransformAdapter(override val sft: SimpleFeatureType,
                               override val auths: Seq[Array[Byte]],
                               val ecql: Filter,
                               val tsft: SimpleFeatureType,
                               val tdefs: String) extends KuduResultAdapter {

    import scala.collection.JavaConverters._

    // determine all the attributes that we need to be able to evaluate the transform and filter
    private val attributes = {
      val fromTransform = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
        case p: PropertyName => Seq(p.getPropertyName)
        case e: Expression   => DataUtilities.attributeNames(e, sft)
      }
      val fromFilter = FilterHelper.propertyNames(ecql, sft)
      (fromTransform ++ fromFilter).distinct
    }

    private val subType = DataUtilities.createSubType(sft, attributes.toArray)
    subType.getUserData.putAll(sft.getUserData)

    private val schema = KuduSimpleFeatureSchema(sft)
    private val deserializer = schema.deserializer(subType)
    private val feature = new ScalaSimpleFeature(subType, "")
    private val transformFeature = TransformSimpleFeature(subType, tsft, tdefs)
    transformFeature.setFeature(feature)

    override val columns: Seq[String] =
      Seq(FeatureIdAdapter.name, VisibilityAdapter.name) ++ schema.schema(attributes).map(_.getName)

    override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
      results.flatMap { row =>
        feature.setId(FeatureIdAdapter.readFromRow(row))
        deserializer.deserialize(row, feature)
        SecurityUtils.setFeatureVisibility(feature, VisibilityAdapter.readFromRow(row))
        if (isVisible(auths, feature) && ecql.evaluate(feature)) {
          Iterator.single(transformFeature)
        } else {
          CloseableIterator.empty
        }
      }
    }

    override def equals(other: Any): Boolean = other match {
      case that: FilterTransformAdapter =>
        sft == that.sft && KuduResultAdapter.equals(auths, that.auths) &&
            filterToString(ecql) == filterToString(that.ecql) && tdefs == that.tdefs
      case _ => false
    }

    override def hashCode(): Int = Seq(sft, auths, ecql, tdefs).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

    override def toString: String =
      s"FilterTransformAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
          s"filter=${filterToString(ecql)}, transform=$tdefs, " +
          s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
  }

  private def binTransform(features: Iterator[SimpleFeature],
                           sft: SimpleFeatureType,
                           trackId: Option[Int],
                           geom: Option[Int],
                           dtg: Option[Int],
                           label: Option[Int],
                           sorting: Boolean): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val encoder = BinaryOutputEncoder(sft, EncodingOptions(geom, dtg, trackId, label))
    val sf = new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "", Array(null, GeometryUtils.zeroPoint))
    val sorted = if (!sorting) { features } else {
      val i = dtg.orElse(sft.getDtgIndex).getOrElse(throw new IllegalArgumentException("Can't sort BIN features by date"))
      features.toList.sortBy(_.getAttribute(i).asInstanceOf[Date]).iterator
    }
    sorted.map { feature =>
      sf.setAttribute(BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX, encoder.encode(feature))
      sf
    }
  }

  private def arrowTransform(original: Iterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             stats: GeoMesaStats,
                             hints: Hints,
                             filter: Option[Filter]): Iterator[SimpleFeature] = {

    import org.locationtech.geomesa.arrow.allocator

    val sort = hints.getArrowSort
    val batchSize = ArrowScan.getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)

    val (features, arrowSft) = hints.getTransform match {
      case None =>
        val sorting = sort.map { case (field, reverse) =>
          if (reverse) { SimpleFeatureOrdering(sft, field).reverse } else { SimpleFeatureOrdering(sft, field) }
        }
        (noTransform(sft, original, sorting), sft)
      case Some((definitions, tsft)) =>
        val sorting = sort.map { case (field, reverse) =>
          if (reverse) { SimpleFeatureOrdering(tsft, field).reverse } else { SimpleFeatureOrdering(tsft, field) }
        }
        (projectionTransform(original, sft, tsft, definitions, sorting), tsft)
    }

    val dictionaryFields = hints.getArrowDictionaryFields
    val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
    val cachedDictionaries: Map[String, TopK[AnyRef]] = if (!hints.isArrowCachedDictionaries) { Map.empty } else {
      def name(i: Int): String = sft.getDescriptor(i).getLocalName
      val toLookup = dictionaryFields.filterNot(providedDictionaries.contains)
      stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => name(k.attribute) -> k).toMap
    }

    if (hints.isArrowDoublePass ||
        dictionaryFields.forall(f => providedDictionaries.contains(f) || cachedDictionaries.contains(f))) {
      // we have all the dictionary values, or we will run a query to determine them up front
      val dictionaries = ArrowScan.createDictionaries(stats, sft, filter, dictionaryFields,
        providedDictionaries, cachedDictionaries)

      val vector = SimpleFeatureVector.create(arrowSft, dictionaries, encoding)
      val batchWriter = new RecordBatchUnloader(vector)

      val sf = ArrowScan.resultFeature()

      val arrows = new Iterator[SimpleFeature] {
        override def hasNext: Boolean = features.hasNext
        override def next(): SimpleFeature = {
          var index = 0
          vector.clear()
          while (index < batchSize && features.hasNext) {
            vector.writer.set(index, features.next)
            index += 1
          }
          sf.setAttribute(0, batchWriter.unload(index))
          sf
        }
      }

      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeBatches(arrowSft, dictionaries, encoding, batchSize, sort)(arrows)
      }
    } else if (hints.isArrowMultiFile) {
      val writer = DictionaryBuildingWriter.create(arrowSft, dictionaryFields, encoding)
      val os = new ByteArrayOutputStream()

      val sf = ArrowScan.resultFeature()

      val arrows = new Iterator[SimpleFeature] {
        override def hasNext: Boolean = features.hasNext
        override def next(): SimpleFeature = {
          writer.clear()
          os.reset()
          var index = 0
          while (index < batchSize && features.hasNext) {
            writer.add(features.next)
            index += 1
          }
          writer.encode(os)
          sf.setAttribute(0, os.toByteArray)
          sf
        }
      }
      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeFiles(arrowSft, dictionaryFields, encoding, sort)(arrows)
      }
    } else {
      val writer = new DeltaWriter(arrowSft, dictionaryFields, encoding, None, batchSize)
      val array = Array.ofDim[SimpleFeature](batchSize)

      val sf = ArrowScan.resultFeature()

      val arrows = new Iterator[SimpleFeature] {
        override def hasNext: Boolean = features.hasNext
        override def next(): SimpleFeature = {
          var index = 0
          while (index < batchSize && features.hasNext) {
            array(index) = features.next
            index += 1
          }
          sf.setAttribute(0, writer.encode(array, index))
          sf
        }
      }
      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeDeltas(arrowSft, dictionaryFields, encoding, batchSize, sort)(arrows)
      }
    }
  }

  private def densityTransform(features: Iterator[SimpleFeature],
                               sft: SimpleFeatureType,
                               envelope: Envelope,
                               width: Int,
                               height: Int,
                               weight: Option[String]): Iterator[SimpleFeature] = {
    val grid = new GridSnap(envelope, width, height)
    val result = scala.collection.mutable.Map.empty[(Int, Int), Double]
    val getWeight = DensityScan.getWeight(sft, weight)
    val writeGeom = DensityScan.writeGeometry(sft, grid)
    features.foreach(f => writeGeom(f, getWeight(f), result))

    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
    // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
    sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(result))
    Iterator(sf)
  }

  private def statsTransform(features: Iterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             transform: Option[(String, SimpleFeatureType)],
                             query: String,
                             encode: Boolean): Iterator[SimpleFeature] = {
    val stat = Stat(sft, query)
    val toObserve = transform match {
      case None                => features
      case Some((tdefs, tsft)) => projectionTransform(features, sft, tsft, tdefs, None)
    }
    toObserve.foreach(stat.observe)
    val encoded = if (encode) { StatsScan.encodeStat(sft)(stat) } else { stat.toJson }
    Iterator(new ScalaSimpleFeature(StatsScan.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint)))
  }

}
