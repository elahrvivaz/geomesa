/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.core
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case class IndexValueEncoder(sft: SimpleFeatureType, fields: Seq[String]) {

  import org.locationtech.geomesa.core.index.IndexValueEncoder._

  fields.foreach(f => require(sft.getDescriptor(f) != null || f == ID_FIELD, s"Encoded field does not exist: $f"))
  val needsBackCompatible = fields == getDefaultSchema(sft) // this may need to decode data encoded the old way

  val types = fields.toIndexedSeq
      .map(f => if (f == ID_FIELD) classOf[String] else normalizeType(sft.getDescriptor(f).getType.getBinding))

  val functions = types.map(t => getFunctions(t, needsBackCompatible))
  val conversions = functions.map(_._1)
  val sizings = functions.map(_._2)
  val encodings = functions.map(_._3)
  val decodings = functions.map(_._4)

  val fieldsWithIndex = fields.zipWithIndex

  def encode(sf: SimpleFeature): Array[Byte] = {
    val valuesWithIndex = fieldsWithIndex.map { case (f, i) =>
      if (f == ID_FIELD) (sf.getID, i) else (sf.getAttribute(f), i)
    }
    var totalSize = 0
    val partialEncodings = valuesWithIndex.map { case (value, i) =>
      val converted = conversions(i)(value)
      totalSize += sizings(i)(converted)
      encodings(i)(converted, _: ByteBuffer)
    }
    val buf = ByteBuffer.allocate(totalSize)
    partialEncodings.foreach(f => f(buf))
    buf.array()
  }

  def decode(value: Array[Byte]): Map[String, Any] = {
    val buf = ByteBuffer.wrap(value)
    fieldsWithIndex.map { case (f, i) => f -> decodings(i)(buf) }.toMap
  }

  val geomField = sft.getGeometryDescriptor.getLocalName
  val dtgField = core.index.getDtgFieldName(sft)

  def id(map: Map[String, Any]): String = map(ID_FIELD).asInstanceOf[String]
  def geometry(map: Map[String, Any]): Geometry = map(geomField).asInstanceOf[Geometry]
  def date(map: Map[String, Any]): Option[Date] =  dtgField.flatMap(f => Option(map(f).asInstanceOf[Date]))
}

object IndexValueEncoder {

  val ID_FIELD = "id"

  private val cache = new ThreadLocal[scala.collection.mutable.Map[String, IndexValueEncoder]] {
    override def initialValue() = scala.collection.mutable.Map.empty
  }

  def apply(sft: SimpleFeatureType) = cache.get.getOrElseUpdate(sft.toString, createNew(sft))

  def getDefaultSchema(sft: SimpleFeatureType): Seq[String] =
    // order is important here, as it needs to match the old IndexEntry encoding
    Seq(ID_FIELD, sft.getGeometryDescriptor.getLocalName) ++ core.index.getDtgFieldName(sft).toSeq

  private def createNew(sft: SimpleFeatureType) = {
    import scala.collection.JavaConversions._
    val schema = {
      val defaults = getDefaultSchema(sft)
      val descriptors =  sft.getAttributeDescriptors
          .filter(d => Option(d.getUserData.get("stidx").asInstanceOf[Boolean]).getOrElse(false))
          .map(_.getLocalName)
      if (descriptors.isEmpty) {
        defaults
      } else {
        defaults ++ descriptors.filterNot(defaults.contains)
      }
    }
    new IndexValueEncoder(sft, schema)
  }

  def encodeSchema(schema: Seq[String]): String = schema.mkString(",")
  def decodeSchema(schema: String): Seq[String] = schema.split(",").map(_.trim)

  private def normalizeType(clas: Class[_]): Class[_] =
    clas match {
      case c if classOf[String].isAssignableFrom(c)              => classOf[String]
      case c if classOf[java.lang.Integer].isAssignableFrom(c)   => classOf[Int]
      case c if classOf[java.lang.Long].isAssignableFrom(c)      => classOf[Long]
      case c if classOf[java.lang.Double].isAssignableFrom(c)    => classOf[Double]
      case c if classOf[java.lang.Float].isAssignableFrom(c)     => classOf[Float]
      case c if classOf[java.lang.Boolean].isAssignableFrom(c)   => classOf[Boolean]
      case c if classOf[UUID].isAssignableFrom(c)                => classOf[UUID]
      case c if classOf[Date].isAssignableFrom(c)                => classOf[Date]
      case c if classOf[Geometry].isAssignableFrom(c)            => classOf[Geometry]
      case _                                                     =>
        throw new IllegalArgumentException(s"Invalid type for index encoding: $clas")
    }

  type GetValueFunction = Any => Any
  type GetSizeFunction =  Any => Int
  type EncodeFunction = (Any, ByteBuffer) => ByteBuffer
  type DecodeFunction = (ByteBuffer) => Any

  private def getFunctions(clas: Class[_], needsBackCompatible: Boolean):
    (GetValueFunction, GetSizeFunction, EncodeFunction, DecodeFunction) =
    clas match {
      case c if c == classOf[String] => (
          (value: Any) =>
            Option(value.asInstanceOf[String]).map(_.getBytes("UTF-8")).getOrElse(Array.empty[Byte]),
          (value: Any) => value.asInstanceOf[Array[Byte]].length + 4,
          (value: Any, buf: ByteBuffer) => {
            val array = value.asInstanceOf[Array[Byte]]
            buf.putInt(array.length).put(array)
          },
          (buf: ByteBuffer) => {
            val length = buf.getInt
            if (length == 0) {
              null
            } else {
              val array = Array.ofDim[Byte](length)
              buf.get(array)
              new String(array, "UTF-8")
            }
          }
        )

      case c if c == classOf[Int] => (
          (value: Any) => Option(value).getOrElse(0),
          (_: Any) => 4,
          (value: Any, buf: ByteBuffer) => buf.putInt(value.asInstanceOf[Int]),
          (buf: ByteBuffer) => buf.getInt
        )

      case c if c == classOf[Long] => (
          (value: Any) => Option(value).getOrElse(0L),
          (_: Any) => 8,
          (value: Any, buf: ByteBuffer) => buf.putLong(value.asInstanceOf[Long]),
          (buf: ByteBuffer) => buf.getLong
        )

      case c if c == classOf[Double] => (
          (value: Any) => Option(value).getOrElse(0d),
          (_: Any) => 8,
          (value: Any, buf: ByteBuffer) => buf.putDouble(value.asInstanceOf[Double]),
          (buf: ByteBuffer) => buf.getDouble
        )

      case c if c == classOf[Float] => (
          (value: Any) => Option(value).getOrElse(0f),
          (_: Any) => 4,
          (value: Any, buf: ByteBuffer) => buf.putFloat(value.asInstanceOf[Float]),
          (buf: ByteBuffer) => buf.getFloat
        )

      case c if c == classOf[Boolean] => (
          (value: Any) => if (Option(value.asInstanceOf[Boolean]).getOrElse(false)) 1.toByte else 0.toByte,
          (_: Any) => 1,
          (value: Any, buf: ByteBuffer) => buf.put(value.asInstanceOf[Byte]),
          (buf: ByteBuffer) => buf.get == 1.toByte
        )

      case c if c == classOf[UUID] => (
          (value: Any) => Option(value),
          (value: Any) => if (value.asInstanceOf[Option[UUID]].isDefined) 17 else 1,
          (value: Any, buf: ByteBuffer) =>
            value.asInstanceOf[Option[UUID]] match {
              case Some(uuid) =>
                buf.put(1.toByte).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)
              case None => buf.put(0.toByte)
            },
          (buf: ByteBuffer) => if (buf.get == 0.toByte) null else new UUID(buf.getLong, buf.getLong)
        )

      case c if c == classOf[Date] => (
          (value: Any) => Option(value.asInstanceOf[Date]).map(_.getTime),
          (value: Any) => if (value.asInstanceOf[Option[Long]].isDefined) 9 else 1,
          (value: Any, buf: ByteBuffer) =>
            value.asInstanceOf[Option[Long]] match {
              case Some(long) =>
                buf.put(1.toByte).putLong(long)
              case None => buf.put(0.toByte)
            },
          if (needsBackCompatible) {
            (buf: ByteBuffer) =>
              if (buf.remaining() == 8) {
                new Date(buf.getLong) // back compatible test, doesn't include null byte check
              } else if (buf.get == 0.toByte) null else new Date(buf.getLong)
          } else {
            (buf: ByteBuffer) => if (buf.get == 0.toByte) null else new Date(buf.getLong)
          }
        )

      case c if c == classOf[Geometry] => (
          (value: Any) => WKBUtils.write(value.asInstanceOf[Geometry]),
          (value: Any) => value.asInstanceOf[Array[Byte]].length + 4,
          (value: Any, buf: ByteBuffer) => {
            val array = value.asInstanceOf[Array[Byte]]
            buf.putInt(array.length).put(array)
          },
          (buf: ByteBuffer) => {
            val length = buf.getInt
            val array = Array.ofDim[Byte](length)
            buf.get(array)
            WKBUtils.read(array)
          }
        )
    }
}