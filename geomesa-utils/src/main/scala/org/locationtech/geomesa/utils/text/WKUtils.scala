/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.text

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.commons.pool.impl.GenericObjectPool

trait WKTUtils {

  private[this] val wktReaders = new ThreadLocal[WKTReader] {
    override def initialValue() = new WKTReader
  }
  private[this] val wktWriters = new ThreadLocal[WKTWriter] {
    override def initialValue() = new WKTWriter
  }

  def read(s: String): Geometry = wktReaders.get.read(s)
  def write(g: Geometry): String = wktWriters.get.write(g)
}

trait WKBUtils {

  private[this] val wkbReaders = new ThreadLocal[WKBReader] {
    override def initialValue() = new WKBReader
  }
  private[this] val wkbWriters = new ThreadLocal[WKBWriter] {
    override def initialValue() = new WKBWriter
  }

  def read(s: String): Geometry = read(s.getBytes)
  def read(b: Array[Byte]): Geometry = wkbReaders.get.read(b)
  def write(g: Geometry): Array[Byte] = wkbWriters.get.write(g)
}

object WKTUtils extends WKTUtils
object WKBUtils extends WKBUtils

