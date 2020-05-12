/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.time.ZonedDateTime
import java.util.Collections

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams.CatalogParam
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit._

import scala.reflect.ClassTag

class AccumuloAuditService extends AuditWriter with AuditReader {

  import AccumuloAuditService.{AccumuloAuditServiceAuthProviderKey, AccumuloAuditServiceConnectorKey}

  import scala.collection.JavaConverters._

  private var writer: AccumuloEventWriter = _
  private var reader: AccumuloEventReader = _
  private var auths: AuthorizationsProvider = _
  private var _table: String = _

  def table: String = _table

  override def init(params: java.util.Map[String, _ <: AnyRef]): Unit = {
    auths = params.get(AccumuloAuditServiceAuthProviderKey).asInstanceOf[AuthorizationsProvider]
    _table = s"${CatalogParam.lookup(params)}_queries"
    val connector = params.get(AccumuloAuditServiceConnectorKey).asInstanceOf[Connector]
    writer = new AccumuloEventWriter(connector, _table)
    reader = new AccumuloEventReader(connector, _table)
  }

  override def writeEvent[T <: AuditedEvent](event: T)(implicit ct: ClassTag[T]): Unit = {
    if (writer != null) {
      writer.queueStat(event)(transform(ct.runtimeClass.asInstanceOf[Class[T]]))
    }
    AuditLogger.writeEvent(event)
  }

  override def getEvents[T <: AuditedEvent](
      typeName: String,
      dates: (ZonedDateTime, ZonedDateTime)
    )(implicit ct: ClassTag[T]): Iterator[T] = {
    val authorizations = new Authorizations(auths.getAuthorizations.asScala: _*)
    val iter = reader.query(typeName, dates, authorizations)(transform(ct.runtimeClass.asInstanceOf[Class[T]]))
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

  val AccumuloAuditServiceConnectorKey     = "geomesa.internal.audit.connector"
  val AccumuloAuditServiceAuthProviderKey  = "geomesa.internal.audit.auths"

  def config(
      params: java.util.Map[String, _ <: AnyRef],
      connector: Connector,
      auths: AuthorizationsProvider): java.util.Map[String, AnyRef] = {
    val map = new java.util.HashMap[String, AnyRef]()
    map.putAll(params)
    map.put(AccumuloAuditServiceConnectorKey, connector)
    map.put(AccumuloAuditServiceAuthProviderKey, auths)
    map
  }

  def apply(
      connector: Connector,
      catalog: String,
      auths: AuthorizationsProvider): AccumuloAuditService = {
    val service = new AccumuloAuditService()
    val map = Collections.singletonMap(AccumuloDataStoreParams.CatalogParam.key, catalog)
    service.init(config(map, connector, auths))
    service
  }
}

