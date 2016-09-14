/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.geojson.index

import java.io.ByteArrayOutputStream

import org.apache.avro.data.Json
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.codehaus.jackson.JsonNode
import org.locationtech.geomesa.api.ValueSerializer

class GeoJsonSerializer extends ValueSerializer[JsonNode] {

  override def toBytes(value: JsonNode): Array[Byte] = GeoJsonSerializer.write(value)

  override def fromBytes(bytes: Array[Byte]): JsonNode = GeoJsonSerializer.read(bytes)
}

object GeoJsonSerializer {

  private val writers = new ThreadLocal[(Json.Writer, ByteArrayOutputStream, BinaryEncoder)] {
    override def initialValue: (Json.Writer, ByteArrayOutputStream, BinaryEncoder) = {
      val buffer = new ByteArrayOutputStream(128)
      (new Json.Writer(), buffer, EncoderFactory.get.binaryEncoder(buffer, null))
    }
  }

  private val readers = new ThreadLocal[(Json.Reader, BinaryDecoder)] {
    override def initialValue: (Json.Reader, BinaryDecoder) =
      (new Json.Reader(), DecoderFactory.get.binaryDecoder(Array.empty[Byte], null))
  }

  def write(json: JsonNode): Array[Byte] = {
    val (writer, buffer, out) = writers.get
    buffer.reset()
    writer.write(json, out)
    out.flush()
    buffer.toByteArray
  }

  def read(bytes: Array[Byte]): JsonNode = {
    val (reader, reuse) = readers.get
    val in = DecoderFactory.get.binaryDecoder(bytes, reuse)
    reader.read(null, in)
  }
}
