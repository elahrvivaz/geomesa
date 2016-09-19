/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.kryo.json

import java.lang.ref.SoftReference

import com.esotericsoftware.kryo.io.{Input, Output}
import org.geotools.factory.Hints
import org.geotools.filter.expression.{PropertyAccessor, PropertyAccessorFactory}
import org.geotools.util.Converters
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{PathAttribute, PathAttributeWildCard, PathDeepScan, PathElement}
import org.opengis.feature.simple.SimpleFeature

import scala.util.control.NonFatal

object JsonPathPropertyAccessor extends PropertyAccessor {

  private val paths = new java.util.concurrent.ConcurrentHashMap[String, SoftReference[Seq[PathElement]]]()

  override def canHandle(obj: Any, xpath: String, target: Class[_]): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val path = try { pathFor(xpath) } catch { case NonFatal(e) => Seq.empty }

    if (path.isEmpty) { false } else {
      val sft = obj.asInstanceOf[SimpleFeature].getFeatureType
      path.head match {
        case PathAttribute(name: String) =>
          val descriptor = sft.getDescriptor(name)
          descriptor != null && descriptor.isJson
        case PathAttributeWildCard | PathDeepScan =>
          import scala.collection.JavaConversions._
          sft.getAttributeDescriptors.exists(_.isJson())
        case _ => false
      }
    }
  }

  override def get[T](obj: Any, xpath: String, target: Class[T]): T = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    val sft = obj.asInstanceOf[SimpleFeature].getFeatureType
    val path = pathFor(xpath)

    val attribute = path.head match {
      case PathAttribute(name: String) => sft.indexOf(name)
      case _ => // we know it will be a wildcard due to canHandle
        import scala.collection.JavaConversions._
        sft.getAttributeDescriptors.indexWhere(_.isJson()) // TODO handle multiple json attributes?
    }

    val input = obj match {
      case f: KryoBufferSimpleFeature => f.getInput(attribute)
      case f: SimpleFeature =>
        // we have to serialize the json string to get an input
        // TODO cache inputs/outputs?
        val jsonString = f.getAttribute(attribute).asInstanceOf[String]
        val out = new Output(if (jsonString == null) 16 else jsonString.length, -1)
        KryoJsonSerialization.serialize(out, jsonString)
        new Input(out.getBuffer, 0, out.position())
    }

    val deserialized = KryoJsonSerialization.deserialize(input, path.tail)
    if (target == null) {
      deserialized.asInstanceOf[T]
    } else {
      Converters.convert(deserialized, target)
    }
  }

  override def set[T](obj: Any, xpath: String, value: T, target: Class[T]): Unit = throw new NotImplementedError()

  private def pathFor(path: String): Seq[PathElement] = {
    val cached = paths.get(path) match {
      case null => null
      case c => c.get
    }
    if (cached != null) { cached } else {
      val parsed = JsonPathParser.parse(path, report = false)
      paths.put(path, new SoftReference(parsed))
      parsed
    }
  }
}

class JsonPropertyAccessorFactory extends PropertyAccessorFactory {

  override def createPropertyAccessor(tipe: Class[_],
                                      xpath: String,
                                      target: Class[_],
                                      hints: Hints): PropertyAccessor = {
    if (classOf[SimpleFeature].isAssignableFrom(tipe) && xpath != null && xpath.startsWith("$.")) {
      JsonPathPropertyAccessor
    } else {
      null
    }
  }
}