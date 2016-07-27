/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.api.index

import org.opengis.feature.simple.SimpleFeature

import scala.util.hashing.MurmurHash3

/**
  * Feature for writing to accumulo
  */
trait WritableFeature[WriteKey] {

  /**
    * Raw feature being written
    *
    * @return
    */
  def feature: SimpleFeature

  /**
    * Main data values
    *
    * @return
    */
  def fullValues: Seq[WriteKey]

  /**
    * Index values - e.g. a trimmed down feature with only date and geometry
    *
    * @return
    */
  def indexValues: Seq[WriteKey]

  /**
    * Pre-computed BIN values
    *
    * @return
    */
  def binValues: Seq[WriteKey]

  /**
    * Hash of the feature ID
    *
    * @return
    */
  lazy val idHash: Int = Math.abs(MurmurHash3.stringHash(feature.getID))
}
