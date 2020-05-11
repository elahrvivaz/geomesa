/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.audit

import java.io.Closeable
import java.time.ZonedDateTime

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

/**
  * Basic trait for any 'event' that we may want to audit. Ties it to a particular data store, schema type name
  * and date
  */
trait AuditedEvent {

  /**
    * Underlying data store type that triggered the event - e.g. 'accumulo', 'hbase', 'kafka'
    *
    * @return
    */
  def storeType: String

  /**
    * Simple feature type name that triggered the event
    *
    * @return
    */
  def typeName: String

  /**
    * Date of event, in millis since the Java epoch
    *
    * @return
    */
  def date: Long

  /**
   * Has the event been marked as deleted?
   *
   * @return
   */
  def deleted: Boolean
}
