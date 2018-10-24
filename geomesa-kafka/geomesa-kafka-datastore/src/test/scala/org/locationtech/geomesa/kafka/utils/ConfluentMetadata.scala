/***********************************************************************
  * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import org.locationtech.geomesa.index.metadata.GeoMesaMetadata

class ConfluentMetadata(schemaRegistry: SchemaRegistry) extends GeoMesaMetadata[String] {

  override def getFeatureTypes: Array[String] = synchronized(schemas.keys.toArray)

  override def read(typeName: String, key: String, cache: Boolean): Option[String] = synchronized {
    schemas.get(typeName).flatMap(_.get(key))
  }

  override def scan(typeName: String, prefix: String, cache: Boolean): Seq[(String, String)] = synchronized {
    schemas.get(typeName) match {
      case None => Seq.empty
      case Some(m) => m.filterKeys(_.startsWith(prefix)).toSeq
    }
  }

  override def invalidateCache(typeName: String, key: String): Unit = {}

  override def close(): Unit = {}



  override def insert(typeName: String, key: String, value: String): Unit =  throw new NotImplementedError()
  override def insert(typeName: String, kvPairs: Map[String, String]): Unit =  throw new NotImplementedError()
  override def remove(typeName: String, key: String): Unit =  throw new NotImplementedError()
  override def delete(typeName: String): Unit =  throw new NotImplementedError()
}
