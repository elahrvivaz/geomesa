/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.kryo.serialization

import java.nio.charset.StandardCharsets

import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import org.json4s.JsonAST.{JNull, _}
import org.json4s.{JObject, JValue}

object KryoJsonSerialization extends LazyLogging {

  private val TerminalByte :Byte = 0x00
  private val DoubleByte   :Byte = 0x01
  private val StringByte   :Byte = 0x02 // e_name string	UTF-8 string
  private val DocByte      :Byte = 0x03 // e_name document	Embedded document
  private val ArrayByte    :Byte = 0x04 // e_name document	Array
  private val BooleanByte  :Byte = 0x08 // e_name 0x00	Boolean "false"
//  private val Byte: Byte = 0x08 e_name 0x01	Boolean "true"
  private val UtcDateByte  :Byte = 0x09 // e_name int64	UTC datetime
  private val NullByte     :Byte = 0x0A // e_name	Null value
  private val IntByte      :Byte = 0x10 // e_name int32	32-bit integer
  private val TimestampByte: Byte = 0x11 // e_name int64	Timestamp
  private val LongByte     :Byte = 0x12 // e_name int64	64-bit integer

  private val BooleanFalse: Byte = 0x00
  private val BooleanTrue: Byte  = 0x01

  def serialize(out: Output, json: String): Unit = {
    import org.json4s._
    import org.json4s.native.JsonMethods._

    serialize(out, parse(json).asInstanceOf[JObject])
  }

  def serialize(out: Output, json: JObject): Unit = {
    val start = out.position()
    out.setPosition(start + 4) // skip space to write total length
    json.obj.foreach { case (name, value) =>
        value match {
          case v: JString  => writeString(out, name, v)
          case v: JObject  => writeDocument(out, name, v)
          case v: JDouble  => writeDouble(out, name, v)
          case v: JInt     => writeInt(out, name, v)
          case v: JArray   => writeArray(out, name, v)
          case JNull       => writeNull(out, name)
          case v: JBool    => writeBoolean(out, name, v)
          case v: JDecimal => writeDecimal(out, name, v)
        }
    }
    out.write(TerminalByte) // marks the end of our object
    // go back and write the total length
    val end = out.position()
    out.setPosition(start)
    out.writeInt(end - start)
    out.setPosition(end)
  }

  def deserialize(in: Input): String = {
    import org.json4s.native.JsonMethods._

    compact(render(deserializeJson(in)))
  }

  def deserializeJson(in: Input): JObject = {

  }


  def deserialize(in: Input, path: String): Option[Any] = {
    // TODO
    None
  }

  private def writeDocument(out: Output, name: String, value: JObject): Unit = {
    out.write(DocByte)
    out.writeName(name)
    serialize(out, value)
  }

  private def writeArray(out: Output, name: String, value: JArray): Unit = {
    out.write(ArrayByte)
    out.writeName(name)
    // we store as an object where array index is the key
    var i = -1
    val withKeys = value.arr.map { element => i += 1; (i.toString, element) } // note: side-effect in map
    serialize(out, JObject(withKeys))
  }

  private def writeString(out: Output, name: String, value: JString): Unit = {
    out.write(StringByte)
    out.writeName(name)
    val bytes = value.values.getBytes(StandardCharsets.UTF_8)
    out.writeInt(bytes.length)
    out.write(bytes)
    out.write(TerminalByte)
  }

  private def writeDecimal(out: Output, name: String, value: JDecimal): Unit = {
    out.write(DoubleByte)
    out.writeName(name)
    out.writeDouble(value.values.toDouble)
  }

  private def writeDouble(out: Output, name: String, value: JDouble): Unit = {
    out.write(DoubleByte)
    out.writeName(name)
    out.writeDouble(value.values)
  }

  private def writeInt(out: Output, name: String, value: JInt): Unit = {
    if (value.values.isValidInt) {
      out.write(IntByte)
      out.writeName(name)
      out.writeInt(value.values.intValue())
    } else if (value.values.isValidLong) {
      out.write(LongByte)
      out.writeName(name)
      out.writeLong(value.values.longValue())
    } else {
      logger.error(s"Skipping int value that does not fit in a long: $value")
    }
  }

  private def writeBoolean(out: Output, name: String, v: JBool): Unit = {
    out.write(BooleanByte)
    out.writeName(name)
    out.write(if (v.values) BooleanTrue else BooleanFalse)
  }

  private def writeNull(out: Output, name: String): Unit = {
    out.write(NullByte)
    out.writeName(name)
  }

  implicit private class RichOutput(val out: Output) extends AnyRef {
    def writeName(name: String): Unit = {
      out.write(name.getBytes(StandardCharsets.UTF_8))
      out.write(TerminalByte)
    }
  }

}

/*

Reduced BSON spec - only native JSON elements supported

byte	1 byte (8-bits)
int32	4 bytes (32-bit signed integer, two's complement)
int64	8 bytes (64-bit signed integer, two's complement)
double	8 bytes (64-bit IEEE 754-2008 binary floating point)

document	::=	int32 e_list "\x00"	BSON Document. int32 is the total number of bytes comprising the document.
e_list	::=	element e_list
          |	""
element	::=	"\x01" e_name double	64-bit binary floating point
          |	"\x02" e_name string	UTF-8 string
          |	"\x03" e_name document	Embedded document
          |	"\x04" e_name document	Array
          |	"\x08" e_name "\x00"	Boolean "false"
          |	"\x08" e_name "\x01"	Boolean "true"
          |	"\x09" e_name int64	UTC datetime
          |	"\x0A" e_name	Null value
          |	"\x10" e_name int32	32-bit integer
          |	"\x11" e_name int64	Timestamp
          |	"\x12" e_name int64	64-bit integer
e_name	::=	cstring	Key name
string	::=	int32 (byte*) "\x00"	String - The int32 is the number bytes in the (byte*) + 1 (for the trailing '\x00'). The (byte*) is zero or more UTF-8 encoded characters.
cstring	::=	(byte*) "\x00"	Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*) MUST NOT contain '\x00', hence it is not full UTF-8.
 */