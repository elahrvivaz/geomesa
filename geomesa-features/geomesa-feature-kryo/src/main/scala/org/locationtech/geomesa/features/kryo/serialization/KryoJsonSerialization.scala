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
import org.json4s.JsonAST._

object KryoJsonSerialization extends LazyLogging {

  private val TerminalByte :Byte = 0x00
  private val DoubleByte   :Byte = 0x01
  private val StringByte   :Byte = 0x02
  private val DocByte      :Byte = 0x03
  private val ArrayByte    :Byte = 0x04
  private val BooleanByte  :Byte = 0x08
  private val NullByte     :Byte = 0x0A
  private val IntByte      :Byte = 0x10
  private val LongByte     :Byte = 0x12

  private val BooleanFalse: Byte = 0x00
  private val BooleanTrue: Byte  = 0x01

  private val nameBuffers = new ThreadLocal[Array[Byte]] {
    override def initialValue(): Array[Byte] = Array.ofDim[Byte](32)
  }

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
        case v: JArray   => writeArray(out, name, v)
        case v: JDouble  => writeDouble(out, name, v)
        case v: JInt     => writeInt(out, name, v)
        case JNull       => writeNull(out, name)
        case v: JBool    => writeBoolean(out, name, v)
        case v: JDecimal => writeDecimal(out, name, v)
      }
    }
    out.writeByte(TerminalByte) // marks the end of our object
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
    val start = in.position()
    val end = in.readInt() + start - 1 // last byte is the terminal byte
    val elements = scala.collection.mutable.ArrayBuffer.empty[JField]
    while (in.position() < end) {
      elements.append(readElement(in))
    }
    in.skip(1) // skip over terminal byte
    JObject(elements.toList)
  }

  def deserialize(in: Input, path: String): Option[Any] = {
    // TODO
    None
  }

  private def readElement(in: Input): JField = {
    val switch = in.read().toByte
    val name = in.readName()
    val value = switch match {
      case StringByte   => readString(in)
      case DocByte      => deserializeJson(in)
      case ArrayByte    => readArray(in)
      case DoubleByte   => readDouble(in)
      case IntByte      => readInt(in)
      case LongByte     => readLong(in)
      case NullByte     => JNull
      case BooleanByte  => readBoolean(in)
    }
    JField(name, value)
  }

  private def writeDocument(out: Output, name: String, value: JObject): Unit = {
    out.writeByte(DocByte)
    out.writeName(name)
    serialize(out, value)
  }

  private def writeArray(out: Output, name: String, value: JArray): Unit = {
    out.writeByte(ArrayByte)
    out.writeName(name)
    // we store as an object where array index is the key
    var i = -1
    val withKeys = value.arr.map { element => i += 1; (i.toString, element) } // note: side-effect in map
    serialize(out, JObject(withKeys))
  }

  private def readArray(in: Input): JArray = JArray(deserializeJson(in).obj.map(_._2))

  private def writeString(out: Output, name: String, value: JString): Unit = {
    out.writeByte(StringByte)
    out.writeName(name)
    val bytes = value.values.getBytes(StandardCharsets.UTF_8)
    out.writeInt(bytes.length)
    out.write(bytes)
    out.writeByte(TerminalByte)
  }

  private def readString(in: Input): JString = {
    val bytes = Array.ofDim[Byte](in.readInt())
    in.read(bytes)
    in.skip(1) // skip TerminalByte
    JString(new String(bytes, StandardCharsets.UTF_8))
  }

  private def writeDecimal(out: Output, name: String, value: JDecimal): Unit = {
    out.writeByte(DoubleByte)
    out.writeName(name)
    out.writeDouble(value.values.toDouble)
  }

  private def writeDouble(out: Output, name: String, value: JDouble): Unit = {
    out.writeByte(DoubleByte)
    out.writeName(name)
    out.writeDouble(value.values)
  }

  private def readDouble(in: Input): JDouble = JDouble(in.readDouble())

  private def writeInt(out: Output, name: String, value: JInt): Unit = {
    if (value.values.isValidInt) {
      out.writeByte(IntByte)
      out.writeName(name)
      out.writeInt(value.values.intValue())
    } else if (value.values.isValidLong) {
      out.writeByte(LongByte)
      out.writeName(name)
      out.writeLong(value.values.longValue())
    } else {
      logger.error(s"Skipping int value that does not fit in a long: $value")
    }
  }

  private def readInt(in: Input): JInt = JInt(in.readInt())

  private def readLong(in: Input): JInt = JInt(in.readLong())

  private def writeBoolean(out: Output, name: String, v: JBool): Unit = {
    out.writeByte(BooleanByte)
    out.writeName(name)
    out.writeByte(if (v.values) BooleanTrue else BooleanFalse)
  }

  private def readBoolean(in: Input): JBool = JBool(in.readByte == BooleanTrue)

  private def writeNull(out: Output, name: String): Unit = {
    out.writeByte(NullByte)
    out.writeName(name)
  }

  private implicit class RichOutput(val out: Output) extends AnyRef {
    def writeName(name: String): Unit = {
      // note: names are not allowed to contain the terminal byte (0x00) but we don't check for it
      out.write(name.getBytes(StandardCharsets.UTF_8))
      out.writeByte(TerminalByte)
    }
  }

  private implicit class RichInput(val in: Input) extends AnyRef {
    def readName(): String = {
      var buffer = nameBuffers.get()
      var i = 0
      var byte: Byte = in.readByte()
      while (byte != TerminalByte) {
        if (i == buffer.length) {
          val copy = Array.ofDim[Byte](buffer.length * 2)
          System.arraycopy(buffer, 0, copy, 0, i)
          buffer = copy
          nameBuffers.set(buffer)
        }
        buffer(i) = byte
        i += 1
        byte = in.readByte()
      }
      new String(buffer, 0, i, StandardCharsets.UTF_8)
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
string	::=	int32 (byte*) "\x00"	String - The int32 is the number bytes in the (byte*) + 1 (for the trailing '\x00').
                                  The (byte*) is zero or more UTF-8 encoded characters.
cstring	::=	(byte*) "\x00"	Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*)
                            MUST NOT contain '\x00', hence it is not full UTF-8.

Note:
  Array - The document for an array is a normal BSON document with integer values for the keys,
  starting with 0 and continuing sequentially. For example, the array ['red', 'blue'] would be
  encoded as the document {'0': 'red', '1': 'blue'}. The keys must be in ascending numerical order.
 */