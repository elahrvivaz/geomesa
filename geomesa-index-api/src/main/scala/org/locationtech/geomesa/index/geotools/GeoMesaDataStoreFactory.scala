/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.Serializable
import java.util.concurrent.TimeUnit

import org.locationtech.geomesa.index.conf.{QueryProperties, StatsProperties}
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider, NullAuditWriter}
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{ConvertedParam, SystemPropertyBooleanParam, SystemPropertyDurationParam}

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

object GeoMesaDataStoreFactory {

  private val GenerateStatsSysParam = SystemPropertyBooleanParam(StatsProperties.GenerateStats)
  private val TimeoutSysParam = SystemPropertyDurationParam(QueryProperties.QueryTimeout)

  private val DeprecatedTimeout =
    ConvertedParam[Duration, java.lang.Long]("queryTimeout", v => Duration(v, TimeUnit.SECONDS))
  private val DeprecatedAccumuloTimeout =
    ConvertedParam[Duration, java.lang.Long]("accumulo.queryTimeout", v => Duration(v, TimeUnit.SECONDS))

  val QueryThreadsParam =
    new GeoMesaParam[Integer](
      "geomesa.query.threads",
      "The number of threads to use per query",
      default = Int.box(8),
      deprecatedKeys = Seq("queryThreads", "accumulo.queryThreads"),
      supportsNiFiExpressions = true)

  val QueryTimeoutParam =
    new GeoMesaParam[Duration](
      "geomesa.query.timeout",
      "The max time a query will be allowed to run before being killed, e.g. '60 seconds'",
      deprecatedParams = Seq(DeprecatedTimeout, DeprecatedAccumuloTimeout),
      systemProperty = Some(TimeoutSysParam),
      supportsNiFiExpressions = true)

  val LooseBBoxParam =
    new GeoMesaParam[java.lang.Boolean](
      "geomesa.query.loose-bounding-box",
      "Use loose bounding boxes - queries will be faster but may return extraneous results",
      default = true,
      deprecatedKeys = Seq("looseBoundingBox"))

  val StrictBBoxParam =
    new GeoMesaParam[java.lang.Boolean](
      "geomesa.query.loose-bounding-box",
      "Use loose bounding boxes - queries will be faster but may return extraneous results",
      default = false,
      deprecatedKeys = Seq("looseBoundingBox"))

  val CachingParam =
    new GeoMesaParam[java.lang.Boolean](
      "geomesa.query.caching",
      "Cache the results of queries for faster repeated searches. Warning: large result sets can swamp memory",
      default = false,
      deprecatedKeys = Seq("caching"))

  val GenerateStatsParam =
    new GeoMesaParam[java.lang.Boolean](
      "geomesa.stats.enable",
      "Generate and persist data statistics for new feature types",
      default = true,
      deprecatedKeys = Seq("generateStats"),
      systemProperty = Some(GenerateStatsSysParam))

  val NamespaceParam = new GeoMesaParam[String]("namespace", "Namespace")

  class AuditQueriesParam(default: String = classOf[AuditLogger].getName)
      extends GeoMesaParam[String](
      "geomesa.query.audit",
      "Class for auditing queries that are run",
      default = default,
      enumerations = ServiceLoader.load[AuditWriter]().map(_.getClass.getName),
      deprecatedParams = Seq("auditQueries", "collectQueryStats").map(new DeprecatedAuditQueries(_))
    ) with ServiceParam[AuditWriter] {

      override protected def ct: ClassTag[AuditWriter] = ClassTag(classOf[AuditWriter])

      override def load(params: java.util.Map[String, _]): Option[AuditWriter] = {
        val opt = super.load(params)
        opt.foreach(_.init(params.asInstanceOf[java.util.Map[String, _ <: AnyRef]]))
        opt
      }

      override def lookUp(map: java.util.Map[String, _]): AnyRef = {
        // handle old true/false configs
        def deprecation(): Unit = deprecationWarning(s"$key: Boolean")
        map.get(key) match {
          case java.lang.Boolean.TRUE | true            => deprecation(); classOf[AuditLogger].getName
          case s: String if s.equalsIgnoreCase("true")  => deprecation(); classOf[AuditLogger].getName
          case java.lang.Boolean.FALSE | false          => deprecation(); classOf[NullAuditWriter].getName
          case s: String if s.equalsIgnoreCase("false") => deprecation(); classOf[NullAuditWriter].getName
          case _ => super.lookUp(map)
        }
      }
    }

  trait ServiceParam[T] extends GeoMesaParam[String] {

    protected def ct: ClassTag[T]

    def load(params: java.util.Map[String, _]): Option[T] = {
      lookupOpt(params).map { name =>
        val all = ServiceLoader.load[T]()(ct)
        all.find(_.getClass.getName == name).getOrElse {
          throw new IllegalArgumentException(
            s"Could not load service class '$name'. Available providers: " +
                all.map(_.getClass.getName).mkString("'", "', '", "'"))
        }
      }
    }
  }

  trait NamespaceConfig {
    def namespace: Option[String]
    def catalog: String
  }

  trait GeoMesaDataStoreConfig extends NamespaceConfig {
    def audit: Option[(AuditWriter, AuditProvider, String)]
    def generateStats: Boolean
    def queries: DataStoreQueryConfig
  }

  trait DataStoreQueryConfig {
    def threads: Int
    def timeout: Option[Long]
    def looseBBox: Boolean
    def caching: Boolean
  }

  // noinspection TypeAnnotation
  trait NamespaceParams {
    val NamespaceParam = GeoMesaDataStoreFactory.NamespaceParam
  }

  // noinspection TypeAnnotation
  trait GeoMesaDataStoreParams extends NamespaceParams {

    protected def looseBBoxDefault = true

    val AuditQueriesParam  = new GeoMesaDataStoreFactory.AuditQueriesParam()
    val GenerateStatsParam = GeoMesaDataStoreFactory.GenerateStatsParam
    val QueryThreadsParam  = GeoMesaDataStoreFactory.QueryThreadsParam
    val QueryTimeoutParam  = GeoMesaDataStoreFactory.QueryTimeoutParam
    val CachingParam       = GeoMesaDataStoreFactory.CachingParam

    val LooseBBoxParam =
      if (looseBBoxDefault) { GeoMesaDataStoreFactory.LooseBBoxParam } else { GeoMesaDataStoreFactory.StrictBBoxParam }
  }

  trait GeoMesaDataStoreInfo {
    def DisplayName: String
    def Description: String
    def ParameterInfo: Array[GeoMesaParam[_]]
    def canProcess(params: java.util.Map[String, _ <: Serializable]): Boolean
  }

  private class DeprecatedAuditQueries(k: String)
      extends ConvertedParam[String, java.lang.Boolean](k,
        v => if (v) { classOf[AuditLogger].getName } else { classOf[NoOpAuditProvider].getName })
}
