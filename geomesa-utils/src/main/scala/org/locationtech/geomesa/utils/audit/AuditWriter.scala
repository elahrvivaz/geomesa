/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.audit

import java.io.Closeable

import scala.reflect.ClassTag

/**
 * Writes an audited event
 */
trait AuditWriter extends Closeable {

  /**
   * Writes an event asynchronously
   *
   * @param event event to write
   * @tparam T event type
   */
  def writeEvent[T <: AuditedEvent](event: T)(implicit ct: ClassTag[T]): Unit
}
