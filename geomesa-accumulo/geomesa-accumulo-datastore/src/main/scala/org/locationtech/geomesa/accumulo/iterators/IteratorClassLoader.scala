/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.geotools.factory.GeoTools

object IteratorClassLoader {

  private var initialized = false
  private val GEOMESA_JAR_NAME = "geomesa"

  def initClassLoader(log: Logger) = synchronized {
    if (!initialized) {
      try {
        log.trace("Initializing classLoader")
        // locate the geomesa jars
        this.getClass.getClassLoader match {
          case vfsCl: VFSClassLoader =>
            vfsCl.getFileObjects.map(_.getURL).filter(_.toString.contains(GEOMESA_JAR_NAME)).foreach { url =>
              if (log != null) {
                log.debug(s"Found geomesa jar at $url")
              }
              val classLoader = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
              GeoTools.addClassLoader(classLoader)
            }

          case _ => // no -op
        }
      } catch {
        case t: Throwable =>
          if (log != null) {
            log.error("Failed to initialize GeoTools' ClassLoader", t)
          }
      } finally {
        initialized = true
      }
    }
  }
}
