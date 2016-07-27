/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.api.utils

import com.typesafe.scalalogging.Logger
import org.locationtech.geomesa.api.index.IndexStrategy
import org.slf4j.LoggerFactory

trait ExplainQuery {
  private var indent = ""
  def apply(s: => String): ExplainQuery = { output(s"$indent$s"); this }
  def apply(s: => String, c: => Seq[String]): ExplainQuery = {
    output(s"$indent$s")
    val ci = c // don't evaluate strings twice
    if (ci.nonEmpty) { pushLevel(); ci.foreach(s => output(s"$indent$s")); popLevel() } else this
  }
  def pushLevel(): ExplainQuery = { indent += "  "; this }
  def pushLevel(s: => String): ExplainQuery = { apply(s); pushLevel(); this }
  def popLevel(): ExplainQuery = { indent = indent.substring(2); this }
  def popLevel(s: => String): ExplainQuery = { popLevel(); apply(s); this }
  protected def output(s: => String)
}

class ExplainPrintln extends ExplainQuery {
  override def output(s: => String): Unit = println(s)
}

object ExplainNull extends ExplainQuery {
  override def apply(s: => String): ExplainQuery = this
  override def pushLevel(): ExplainQuery = this
  override def pushLevel(s: => String): ExplainQuery = this
  override def popLevel(): ExplainQuery = this
  override def popLevel(s: => String): ExplainQuery = this
  override def output(s: => String): Unit = {}
}

class ExplainString extends ExplainQuery {
  private val string: StringBuilder = new StringBuilder()
  override def output(s: => String): Unit = string.append(s).append("\n")
  override def toString = string.toString()
}

class ExplainLogging extends ExplainQuery {
  override def output(s: => String): Unit = ExplainLogging.logger.trace(s)
}

object ExplainLogging {
  private val logger = Logger(LoggerFactory.getLogger(IndexStrategy.getClass))
}