/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.index

import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.quadtree.Quadtree

/**
 * Thread safe quad tree
 */
class WrappedQuadtree[T] extends SpatialIndex[T] with Serializable {

  private val qt = new Quadtree

  override def query(envelope: Envelope) = qt.query(envelope).iterator().asInstanceOf[Iterator[T]]

  override def insert(envelope: Envelope, item: T) = qt.insert(envelope, item)

  override def remove(envelope: Envelope, item: T) = qt.remove(envelope, item)
}
