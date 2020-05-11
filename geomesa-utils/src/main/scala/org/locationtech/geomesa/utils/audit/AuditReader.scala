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

import scala.reflect.ClassTag

/**
 * Reads an audited event
 */
trait AuditReader extends Closeable {

  /**
   * Retrieves stored events
   *
   * @param typeName simple feature type name
   * @param dates dates to retrieve stats for
   * @tparam T event type
   * @return iterator of events
   */
  def getEvents[T <: AuditedEvent](
      typeName: String,
      dates: (ZonedDateTime, ZonedDateTime)
    )(implicit ct: ClassTag[T]): Iterator[T]
}
