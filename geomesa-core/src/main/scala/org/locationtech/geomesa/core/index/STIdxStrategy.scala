/*
 * Copyright 2014-2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import java.util.Date
import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.GEOMESA_ITERATORS_IS_DENSITY_TYPE
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.util.{SelfClosingBatchScanner, SelfClosingIterator}
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.Literal
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Intersects}

import scala.util.Try

class STIdxStrategy extends Strategy with Logging with IndexFilterHelpers {

  def execute(acc: AccumuloConnectorCreator,
              iqp: QueryPlanner,
              featureType: SimpleFeatureType,
              query: Query,
              output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val tryScanner = Try {
      val bs = acc.createSTIdxScanner(featureType)
      val version = acc.geomesaVersion(featureType)
      val qp = buildSTIdxQueryPlan(query, iqp, featureType, version, output)
      configureBatchScanner(bs, qp)
      // NB: Since we are (potentially) gluing multiple batch scanner iterators together,
      //  we wrap our calls in a SelfClosingBatchScanner.
      SelfClosingBatchScanner(bs)
    }
    val scanner = tryScanner.recover {
      case e: Throwable =>
        logger.warn(s"Error in creating scanner: $e", e)
        // since GeoTools would eat the error and return no records anyway,
        // there's no harm in returning an empty iterator.
        SelfClosingIterator[Entry[Key, Value]](Iterator.empty)
    }
    scanner.get
  }

  def buildSTIdxQueryPlan(query: Query,
                          iqp: QueryPlanner,
                          featureType: SimpleFeatureType,
                          version: Int,
                          output: ExplainerOutputType) = {
    val schema          = iqp.stSchema
    val featureEncoding = iqp.featureEncoding
    val keyPlanner      = IndexSchema.buildKeyPlanner(schema)
    val cfPlanner       = IndexSchema.buildColumnFamilyPlanner(schema)

    output(s"Scanning ST index table for feature type ${featureType.getTypeName}")
    output(s"Filter: ${query.getFilter}")

     val dtgField = getDtgFieldName(featureType)

    // TODO: Select only the geometry filters which involve the indexed geometry type.
    // https://geomesa.atlassian.net/browse/GEOMESA-200
    // Simiarly, we should only extract temporal filters for the index date field.
    val (geomFilters, otherFilters) = partitionGeom(query.getFilter, featureType)
    val (temporalFilters, ecqlFilters) = partitionTemporal(otherFilters, dtgField)

    val ecql = filterListAsAnd(ecqlFilters)

    output(s"Geometry filters: $geomFilters")
    output(s"Temporal filters: $temporalFilters")
    output(s"Other filters: $ecqlFilters")

    val tweakedGeoms = geomFilters.map(updateTopologicalFilters(_, featureType))

    output(s"Tweaked geom filters are $tweakedGeoms")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tweakedGeoms.flatMap {
      case bbox: BBOX =>
        val bboxPoly = bbox.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])
        Seq(bboxPoly)
      case gf: BinarySpatialOperator =>
        extractGeometry(gf)
      case _ => Seq()
    }

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val temporal = extractTemporal(dtgField)(temporalFilters)
    val interval = netInterval(temporal)
    val geometryToCover = netGeom(collectionToCover)
    val filter = buildFilter(geometryToCover, interval)

    val geomPredsSkippable = tweakedGeoms.forall(g => g.isInstanceOf[BBOX] || g.isInstanceOf[Intersects])
    val (geomsToSkip, datesToSkip) = if (!geomPredsSkippable) (None, None) else filter match {
      // we can only skip rows if both the date and geom are skippable
      case SpatialFilter(geom) => (Some(geomsToCover), None)
      case DateRangeFilter(start, end) => (None, Some(start.toDate, end.toDate))
      case SpatialDateRangeFilter(geom, start, end) => (Some(geomsToCover), Some(start.toDate, end.toDate))
      case AcceptEverythingFilter => (None, None)
      case DateFilter(_) => (None, None) // not a date range, so won't cover a whole row
      case SpatialDateFilter(_, _) => (None, None) // not a date range, so won't cover a whole row
    }

    output(s"GeomsToCover: $geomsToCover")

    val ofilter = filterListAsAnd(tweakedGeoms ++ temporalFilters)
    if (ofilter.isEmpty) logger.warn(s"Querying Accumulo without ST filter.")

    val oint  = IndexSchema.somewhen(interval)

    output(s"STII Filter: ${ofilter.getOrElse("No STII Filter")}")
    output(s"Interval:  ${oint.getOrElse("No interval")}")
    output(s"Filter: ${Option(filter).getOrElse("No Filter")}")

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    val stiiIterCfg = getSTIIIterCfg(iteratorConfig, query, featureType, ofilter, ecql, featureEncoding,
      geomsToSkip, datesToSkip, schema, version)

    val densityIterCfg = getDensityIterCfg(query, geometryToCover, schema, featureEncoding, featureType)

    // set up row ranges and regular expression filter
    val qp = planQuery(filter, iteratorConfig.iterator, output, keyPlanner, cfPlanner)

    qp.copy(iterators = qp.iterators ++ List(Some(stiiIterCfg), densityIterCfg).flatten)
  }

  def getSTIIIterCfg(iteratorConfig: IteratorConfig,
                     query: Query,
                     featureType: SimpleFeatureType,
                     stFilter: Option[Filter],
                     ecqlFilter: Option[Filter],
                     featureEncoding: FeatureEncoding,
                     geomToSkip: Option[Seq[Geometry]],
                     datesToSkip: Option[(Date, Date)],
                     schema: String,
                     version: Int): IteratorSetting = {
    iteratorConfig.iterator match {
      case IndexOnlyIterator =>
        configureIndexIterator(featureType, query, featureEncoding, stFilter,
          iteratorConfig.transformCoversFilter, geomToSkip, datesToSkip, schema, version)
      case SpatioTemporalIterator =>
        val isDensity = query.getHints.containsKey(DENSITY_KEY)
        configureSpatioTemporalIntersectingIterator(featureType, query, featureEncoding, stFilter,
          ecqlFilter, geomToSkip, datesToSkip, schema, isDensity)
    }
  }


  // establishes the regular expression that defines (minimally) acceptable rows
  def configureRowRegexIterator(regex: String): IteratorSetting = {
    val name = "regexRow-" + randomPrintableString(5)
    val cfg = new IteratorSetting(iteratorPriority_RowRegex, name, classOf[RegExFilter])
    RegExFilter.setRegexs(cfg, regex, null, null, null, false)
    cfg
  }

  // returns an iterator over [key,value] pairs where the key is taken from the index row and the value is a SimpleFeature,
  // which is either read directory from the data row  value or generated from the encoded index row value
  // -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureIndexIterator(
      featureType: SimpleFeatureType,
      query: Query,
      featureEncoding: FeatureEncoding,
      stFilter: Option[Filter],
      transformsCoverFilter: Boolean,
      geomsToSkip: Option[Seq[Geometry]],
      datesToSkip: Option[(Date, Date)],
      schema: String,
      version: Int): IteratorSetting = {

    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator, classOf[IndexIterator])

    configureStFilter(cfg, stFilter)
    configureCoveredRows(cfg, geomsToSkip, datesToSkip, schema)
    configureVersion(cfg, version)
    if (transformsCoverFilter) {
      // apply the transform directly to the index iterator
      val testType = query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
      configureFeatureType(cfg, testType)
    } else {
      // we need to evaluate the original feature before transforming
      // transforms are applied afterwards
      configureFeatureType(cfg, featureType)
      configureTransforms(cfg, query)
    }
    configureIndexValues(cfg, featureType)
    configureFeatureEncoding(cfg, featureEncoding)
    cfg
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSpatioTemporalIntersectingIterator(
      featureType: SimpleFeatureType,
      query: Query,
      featureEncoding: FeatureEncoding,
      stFilter: Option[Filter],
      ecqlFilter: Option[Filter],
      geomsToSkip: Option[Seq[Geometry]],
      datesToSkip: Option[(Date, Date)],
      schema: String,
      isDensity: Boolean): IteratorSetting = {

    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      classOf[SpatioTemporalIntersectingIterator])

    (stFilter, ecqlFilter) match {
      case (None, None) => // nothing

      case (None, Some(_)) =>
        configureEcqlFilter(cfg, ecqlFilter.map(ECQL.toCQL))

      case (Some(st), Some(ecql)) =>
        // we might as well combine the filters so we only evaluate once
        configureEcqlFilter(cfg, filterListAsAnd(Seq(st, ecql)).map(ECQL.toCQL))

      case (Some(_), None) =>
        // configure just the st filter, as we can skip it with the covering geoms/dates
        configureStFilter(cfg, stFilter)
        configureCoveredRows(cfg, geomsToSkip, datesToSkip, schema)
    }
    configureFeatureType(cfg, featureType)
    configureFeatureEncoding(cfg, featureEncoding)
    configureTransforms(cfg, query)
    if (isDensity) {
      cfg.addOption(GEOMESA_ITERATORS_IS_DENSITY_TYPE, "isDensity")
    }
    cfg
  }

  def planQuery(filter: KeyPlanningFilter,
                iter: IteratorChoice,
                output: ExplainerOutputType,
                keyPlanner: KeyPlanner,
                cfPlanner: ColumnFamilyPlanner): QueryPlan = {
    output(s"Planning query")

    val indexOnly = iter match {
      case IndexOnlyIterator      => true
      case SpatioTemporalIterator => false
    }

    val keyPlan = keyPlanner.getKeyPlan(filter, indexOnly, output)

    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }

    output(s"Total ranges: ${accRanges.size} - ${accRanges.take(5)}")

    // always try to set a RowID regular expression
    //@TODO this is broken/disabled as a result of the KeyTier
    val iters =
      keyPlan.toRegex match {
        case KeyRegex(regex) => Seq(configureRowRegexIterator(regex))
        case _               => Seq()
      }

    // if you have a list of distinct column-family entries, fetch them
    val cf = columnFamilies match {
      case KeyList(keys) =>
        output(s"ColumnFamily Planner: ${keys.size} : ${keys.take(20)}")
        keys.map { cf => new Text(cf) }

      case _ =>
        Seq()
    }

    QueryPlan(iters, accRanges, cf)
  }
}

object STIdxStrategy extends StrategyProvider {

  import org.locationtech.geomesa.core.filter.spatialFilters
  import org.locationtech.geomesa.core.index.Strategy._

  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints) =
    if (spatialFilters(filter)) {
      val e1 = filter.asInstanceOf[BinarySpatialOperator].getExpression1
      val e2 = filter.asInstanceOf[BinarySpatialOperator].getExpression2
      checkOrder(e1, e2)
          .filter(p => p.name == sft.getGeometryDescriptor.getLocalName)
          .map(_ => StrategyDecision(new STIdxStrategy, -1))
    } else {
      None
    }
}
