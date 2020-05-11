/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.audit

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

/**
 * Implemented AuditWriter by logging events as json
 */
class AuditLogger extends AuditWriter with LazyLogging {

  private val gson: Gson = new GsonBuilder().serializeNulls().create()

  override def init(params: java.util.Map[String, _ <: AnyRef]): Unit = {}

  override def writeEvent[T <: AuditedEvent](event: T)(implicit ct: ClassTag[T]): Unit =
    logger.debug(gson.toJson(event))

  override def close(): Unit = {}
}

object AuditLogger extends AuditLogger
