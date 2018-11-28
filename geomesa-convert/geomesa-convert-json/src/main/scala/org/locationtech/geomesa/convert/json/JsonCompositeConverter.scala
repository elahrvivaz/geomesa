/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.io.{ByteArrayInputStream, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import com.google.gson.JsonElement
import com.jayway.jsonpath.JsonPath
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.Modes.ParseMode
import org.locationtech.geomesa.convert.json.JsonConverter.{JsonConfig, JsonField}
import org.locationtech.geomesa.convert.{Counter, EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicOptions, ErrorHandlingIterator}
import org.locationtech.geomesa.convert2.composite.CompositeConverter.CompositeEvaluationContext
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.convert2.{AbstractConverter, SimpleFeatureConverter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.Try

class JsonCompositeConverter(val targetSft: SimpleFeatureType,
                             options: BasicOptions,
                             delegates: Seq[(Predicate, SimpleFeatureConverter)]) extends SimpleFeatureConverter {

  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

  private val predicates = delegates.mapWithIndex { case ((p, _), i) => (p, i) }.toIndexedSeq
  private val converters = delegates.map(_._2).toIndexedSeq

  override def createEvaluationContext(globalParams: Map[String, Any],
                                       caches: Map[String, EnrichmentCache],
                                       counter: Counter): EvaluationContext = {
    new CompositeEvaluationContext(converters.map(_.createEvaluationContext(globalParams, caches, counter)))
  }


  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val iter = new ErrorHandlingIterator(read(is, ec), ec.counter, options.errorMode, logger)
    val converted = iter.flatMap(convert(_, ec))
    options.parseMode match {
      case ParseMode.Incremental => converted
      case ParseMode.Batch => CloseableIterator(converted.to[ListBuffer].iterator, converted.close())
    }
  }

  override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    val elements = JsonConverter.parse(new InputStreamReader(is, options.encoding), ec)

    val setEc: Int => Unit = ec match {
      case c: CompositeEvaluationContext => i => c.setCurrent(i)
      case _ => _ => Unit
    }
    val toEval = Array.ofDim[Any](1)

    def evalPred(pi: (Predicate, Int)): Boolean = {
      setEc(pi._2)
      Try(pi._1.eval(toEval)(ec)).getOrElse(false)
    }

    val lines = IOUtils.lineIterator(is, StandardCharsets.UTF_8)

    new CloseableIterator[SimpleFeature] {

      private var delegate: CloseableIterator[SimpleFeature] = CloseableIterator.empty

      @tailrec
      override def hasNext: Boolean = delegate.hasNext || {
        if (!lines.hasNext) {
          false
        } else {
          toEval(0) = lines.next()
          delegate.close()
          delegate = predicates.find(evalPred).map(_._2) match {
            case None =>
              ec.counter.incLineCount()
              ec.counter.incFailure()
              CloseableIterator.empty

            case Some(i) =>
              val in = new ByteArrayInputStream(toEval(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
              converters(i).process(in, ec)
          }
          hasNext
        }
      }

      override def next(): SimpleFeature = if (hasNext) { delegate.next } else { Iterator.empty.next }

      override def close(): Unit = {
        CloseWithLogging(delegate)
        is.close()
      }
    }
  }

  override def close(): Unit = converters.foreach(CloseWithLogging.apply)
}

object JsonCompositeConverter {

  class JsonDeleteConverter(targetSft: SimpleFeatureType,
                            config: JsonConfig,
                            fields: Seq[JsonField],
                            options: BasicOptions)
      extends AbstractConverter(targetSft, config, fields, options) {

    private val featurePath = config.featurePath.map(JsonPath.compile(_))

    def process(elements: CloseableIterator[JsonElement], ec: EvaluationContext): CloseableIterator[SimpleFeature] =
      JsonConverter.toValues(elements, featurePath).flatMap(convert(_, ec))

    override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] =
      throw new NotImplementedError()
  }
}
