/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.confluent

import java.io.{InputStream, OutputStream}
import java.net.URL
import java.util.Date

import com.vividsolutions.jts.io.WKTReader
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.features.confluent.ConfluentFeatureSerializer._
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.{ScalaSimpleFeatureFactory, SimpleFeatureSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._

object ConfluentFeatureSerializer {
  def builder(sft: SimpleFeatureType, schemaRegistryUrl: URL): Builder =
    new Builder(sft, schemaRegistryUrl)

  class Builder private [ConfluentFeatureSerializer] (sft: SimpleFeatureType, schemaRegistryUrl: URL)
      extends SimpleFeatureSerializer.Builder[Builder] {

    override def build(): ConfluentFeatureSerializer =
      new ConfluentFeatureSerializer(sft,
                                     new CachedSchemaRegistryClient(schemaRegistryUrl.toExternalForm, 100),
                                     options.toSet)
  }
  val geomAttributeName = "_geom"
  val dateAttributeName = "_date"
}

class ConfluentFeatureSerializer(sft: SimpleFeatureType,
                                 schemaRegistryClient: SchemaRegistryClient,
                                 val options: Set[SerializationOption] = Set.empty)
    extends SimpleFeatureSerializer {

  private val kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  private val wktReader = new WKTReader()

  override def deserialize(id: String, bytes: Array[Byte], timestamp: Option[Date]): SimpleFeature = {
    val genericRecord = kafkaAvroDeserializer.deserialize("", bytes).asInstanceOf[GenericRecord]
    val attrs = sft.getAttributeDescriptors.asScala.map(_.getLocalName).map { attrName =>
      if (attrName == geomAttributeName) {
        wktReader.read(genericRecord.get(attrName).toString)
      } else if (attrName == dateAttributeName) {
        timestamp.get
      } else {
        genericRecord.get(attrName)
      }
    }
    ScalaSimpleFeatureFactory.buildFeature(sft, attrs, id)
  }

  // Implement the following if we find we need them

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError()

  override def deserialize(bytes: Array[Byte]): SimpleFeature = throw new NotImplementedError()

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    throw new NotImplementedError()

  override def deserialize(id: String, in: InputStream): SimpleFeature =
    throw new NotImplementedError()

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    throw new NotImplementedError()

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError("ConfluentSerializer is read-only")
}
