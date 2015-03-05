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

package org.locationtech.geomesa.core.index

import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text

sealed trait TextExtractor {
  def extract(key: Key): String
  def extract(key: String): String
  protected def extract(keyPart: String, offset: Int, bits: Int): String = keyPart.substring(offset, offset + bits)
}

abstract class AbstractExtractor(offset: Int, bits: Int) extends TextExtractor {
  def keyFunction(k: Key): String
  def extract(k: Key): String = extract(keyFunction(k), offset, bits)
  def extract(k: String): String = extract(k, offset, bits)
}

case class RowExtractor(offset: Int, bits: Int) extends AbstractExtractor(offset, bits) {
  override def keyFunction(k: Key) = k.getRow.toString
}

case class ColumnFamilyExtractor(offset: Int, bits: Int) extends AbstractExtractor(offset, bits) {
  override def keyFunction(k: Key) = k.getColumnFamily.toString
}

case class ColumnQualifierExtractor(offset: Int, bits: Int) extends AbstractExtractor(offset, bits) {
  override def keyFunction(k: Key) = k.getColumnQualifier.toString
}

case class KeyExtractor(keyParts: Seq[TextExtractor], bits: Int) extends TextExtractor {
  override def extract(k: Key): String = keyParts.map(_.extract(k)).mkString("")
  override def extract(k: String): String = keyParts.map(_.extract(k)).mkString("")
}