/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.time.ZonedDateTime
import java.util

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams.CatalogParam
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit._

import scala.reflect.ClassTag

class AccumuloAuditService(connector: Connector,
                           authProvider: AuthorizationsProvider,
                           val table: String,
                           write: Boolean) extends AuditWriter with AuditReader {

  import AccumuloAuditService.{AccumuloAuditServiceConnectorKey, AccumuloAuditServiceAuditProviderKey}

  private var writer: AccumuloEventWriter = _
  private val reader = new AccumuloEventReader(connector, table)

  override def init(params: java.util.Map[String, _ <: AnyRef]): Unit = {
    val connector = params.get(AccumuloAuditServiceConnectorKey).asInstanceOf[Connector]
    val provider = params.get(AccumuloAuditServiceAuditProviderKey).asInstanceOf[AuthorizationsProvider]
    val catalog = CatalogParam.lookup(params)
        = if (write) { new AccumuloEventWriter(connector, table) } else { null }
  }

  override def writeEvent[T <: AuditedEvent](event: T)(implicit ct: ClassTag[T]): Unit = {
    if (writer != null) {
      writer.queueStat(event)(transform(ct.runtimeClass.asInstanceOf[Class[T]]))
    }
    super.writeEvent(event)
  }

  override def getEvents[T <: AuditedEvent](typeName: String,
                                            dates: (ZonedDateTime, ZonedDateTime))
                                           (implicit ct: ClassTag[T]): Iterator[T] = {
    import scala.collection.JavaConverters._
    val auths = new Authorizations(authProvider.getAuthorizations.asScala: _*)
    val iter = reader.query(typeName, dates, auths)(transform(ct.runtimeClass.asInstanceOf[Class[T]]))
    iter.asInstanceOf[Iterator[T]]
  }

  override def close(): Unit = if (writer != null) { writer.close() }

  // note: only query audit events are currently supported
  private def transform[T <: AuditedEvent](clas: Class[T]): AccumuloEventTransform[T] = {
    val transform = clas match {
      case c if classOf[QueryEvent].isAssignableFrom(c) => AccumuloQueryEventTransform
      case c if classOf[SerializedQueryEvent].isAssignableFrom(c) => SerializedQueryEventTransform
      case _ => throw new NotImplementedError(s"Event of type '${clas.getName}' is not supported")
    }
    transform.asInstanceOf[AccumuloEventTransform[T]]
  }
}

object AccumuloAuditService {

  val StoreType = "accumulo-vector"

  val AccumuloAuditServiceConnectorKey = "geomesa.internal.audit.connector"
  val AccumuloAuditServiceAuditProviderKey = "geomesa.internal.audit.connector"

  def config(
      params: java.util.Map[String, _ <: AnyRef],
      connector: Connector,
      provider: AuditProvider): java.util.Map[String, AnyRef] = {
    val map = new java.util.HashMap[String, AnyRef]()
    map.putAll(params)
    map.put(AccumuloAuditServiceConnectorKey, connector)
    map.put(AccumuloAuditServiceAuditProviderKey, provider)
    map
  }
}

