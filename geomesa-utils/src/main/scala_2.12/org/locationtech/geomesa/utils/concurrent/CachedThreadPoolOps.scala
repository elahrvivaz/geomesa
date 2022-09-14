/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import java.util.concurrent.Future

/**
 * Scala version bridge for cached thread pool
 */
object CachedThreadPoolOps {

  /**
   * Execute a single command in a potentially cached thread
   *
   * @param command command
   */
  def execute(command: Runnable): Unit = CachedThreadPool.execute(command)

  /**
   * Submit a single command to run in a potentially cached thread
   *
   * @param command command
   * @return
   */
  def submit(command: Runnable): Future[_] = CachedThreadPool.submit(command)
}
