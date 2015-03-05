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

import java.util.Map.Entry
import java.util.{Calendar, Date}

import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.commons.lang.time.DateUtils
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Interval
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators.{FEATURE_ENCODING, _}
import org.locationtech.geomesa.core.util.SelfClosingIterator
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Expression, Literal, PropertyName}

import scala.collection.JavaConversions._
import scala.util.Random

trait Strategy {
  def execute(acc: AccumuloConnectorCreator,
              iqp: QueryPlanner,
              featureType: SimpleFeatureType,
              query: Query,
              output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]]

  def configureBatchScanner(bs: BatchScanner, qp: QueryPlan) {
    qp.iterators.foreach { i => bs.addScanIterator(i) }
    bs.setRanges(qp.ranges)
    qp.cf.foreach { c => bs.fetchColumnFamily(c) }
  }

  def configureFeatureEncoding(cfg: IteratorSetting, featureEncoding: FeatureEncoding) {
    cfg.addOption(FEATURE_ENCODING, featureEncoding.toString)
  }

  def configureStFilter(cfg: IteratorSetting, filter: Option[Filter]) = {
    filter.foreach { f => cfg.addOption(ST_FILTER_PROPERTY_NAME, ECQL.toCQL(f)) }
  }

  def configureCoveredRows(cfg: IteratorSetting,
                           geometries: Option[Seq[Geometry]],
                           dates: Option[(Date, Date)],
                           schema: String) = {

    val roundedDates =
      // round the start/end dates so that we only get fully covered days
      // TODO this assumes that the index schema is to the day, revisit...
      dates.map { case (s, e) => (DateUtils.ceiling(s, Calendar.DATE), DateUtils.truncate(e, Calendar.DATE)) }
        .filter { case (s, e) => s.before(e) } // ensure we have at least 1 day covered
        .flatMap { case (s, e) => // turn the dates into strings
          IndexSchema.buildDateDecoder(schema).map(d => (d.parser.print(s.getTime), d.parser.print(e.getTime)))
        }

    lazy val geohashes = geometries.flatMap { geoms =>
      // TODO test 3 vs 2 chars of precision
      val geohashLength = Math.min(3, IndexSchema.buildGeohashRowExtractor(schema).bits)
      val coveringGeohashes = GeohashUtils.getCoveredGeohashes(geoms, geohashLength)
      if (coveringGeohashes.isEmpty) None else Some(coveringGeohashes)
    }

    // if both the dates and geoms passed in result in skippable rows, then we can skip, otherwise we can't
    val canSkip = roundedDates.isDefined == dates.isDefined && geohashes.isDefined == geometries.isDefined &&
        (roundedDates.isDefined || geohashes.isDefined)

    if (canSkip) {
      // set the values in the config
      configureKeySchema(cfg, schema)
      geohashes.foreach(gh => cfg.addOption(GEOMESA_ITERATORS_COVERED_GEOS, gh.mkString(",")))
      roundedDates.foreach { case (s, e) =>
        cfg.addOption(GEOMESA_ITERATORS_COVERED_DATE_START, s)
        cfg.addOption(GEOMESA_ITERATORS_COVERED_DATE_END, e)
      }
    }
  }

  def configureKeySchema(cfg: IteratorSetting, schema: String) =
    cfg.addOption(GEOMESA_ITERATORS_KEY_SCHEMA, schema)

  def configureVersion(cfg: IteratorSetting, version: Int) =
    cfg.addOption(GEOMESA_ITERATORS_VERSION, version.toString)

  def configureFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  def configureFeatureTypeName(cfg: IteratorSetting, featureType: String) =
    cfg.addOption(GEOMESA_ITERATORS_SFT_NAME, featureType)

  def configureIndexValues(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SFT_INDEX_VALUE, encodedSimpleFeatureType)
  }

  def configureEcqlFilter(cfg: IteratorSetting, ecql: Option[String]) =
    ecql.foreach(filter => cfg.addOption(GEOMESA_ITERATORS_ECQL_FILTER, filter))

  // returns the SimpleFeatureType for the query's transform
  def transformedSimpleFeatureType(query: Query): Option[SimpleFeatureType] = {
    Option(query.getHints.get(TRANSFORM_SCHEMA)).map {_.asInstanceOf[SimpleFeatureType]}
  }

  // store transform information into an Iterator's settings
  def configureTransforms(cfg: IteratorSetting, query:Query) =
    for {
      transformOpt  <- Option(query.getHints.get(TRANSFORMS))
      transform     = transformOpt.asInstanceOf[String]
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM, transform)
      sfType        <- transformedSimpleFeatureType(query)
      encodedSFType = SimpleFeatureTypes.encodeType(sfType)
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM_SCHEMA, encodedSFType)
    } yield Unit

  def configureRecordTableIterator(
      simpleFeatureType: SimpleFeatureType,
      featureEncoding: FeatureEncoding,
      ecql: Option[Filter],
      query: Query): IteratorSetting = {

    val cfg = new IteratorSetting(
      iteratorPriority_SimpleFeatureFilteringIterator,
      classOf[RecordTableIterator].getSimpleName,
      classOf[RecordTableIterator]
    )
    configureFeatureType(cfg, simpleFeatureType)
    configureFeatureEncoding(cfg, featureEncoding)
    configureEcqlFilter(cfg, ecql.map(ECQL.toCQL))
    configureTransforms(cfg, query)
    cfg
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def getDensityIterCfg(query: Query,
                    geometryToCover: Geometry,
                    schema: String,
                    featureEncoding: FeatureEncoding,
                    featureType: SimpleFeatureType) = query match {
    case _ if query.getHints.containsKey(DENSITY_KEY) =>
      val clazz = classOf[DensityIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val width = query.getHints.get(WIDTH_KEY).asInstanceOf[Int]
      val height = query.getHints.get(HEIGHT_KEY).asInstanceOf[Int]
      val polygon = if (geometryToCover == null) null else geometryToCover.getEnvelope.asInstanceOf[Polygon]

      DensityIterator.configure(cfg, polygon, width, height)

      cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ if query.getHints.containsKey(TEMPORAL_DENSITY_KEY) =>
      val clazz = classOf[TemporalDensityIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val interval = query.getHints.get(TIME_INTERVAL_KEY).asInstanceOf[Interval]
      val buckets = query.getHints.get(TIME_BUCKETS_KEY).asInstanceOf[Int]

      TemporalDensityIterator.configure(cfg, interval, buckets)

      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ if query.getHints.containsKey(MAP_AGGREGATION_KEY) =>
      val clazz = classOf[MapAggregatingIterator]

      val cfg = new IteratorSetting(iteratorPriority_AnalysisIterator,
        "topfilter-" + randomPrintableString(5),
        clazz)

      val mapAttribute = query.getHints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]

      MapAggregatingIterator.configure(cfg, mapAttribute)

      configureFeatureEncoding(cfg, featureEncoding)
      configureFeatureType(cfg, featureType)

      Some(cfg)
    case _ => None
  }
}

object Strategy {

  /**
   * Checks the order of properties and literals in the expression
   *
   * @param one
   * @param two
   * @return (prop, literal, whether the order was flipped)
   */
  def checkOrder[T](one: Expression, two: Expression): Option[PropertyLiteral] =
    (one, two) match {
      case (p: PropertyName, l: Literal) => Some(PropertyLiteral(p.getPropertyName, l, None, false))
      case (l: Literal, p: PropertyName) => Some(PropertyLiteral(p.getPropertyName, l, None, true))
      case (_: PropertyName, _: PropertyName) | (_: Literal, _: Literal) => None
      case _ =>
        val msg = s"Unhandled expressions in strategy: ${one.getClass.getName}, ${two.getClass.getName}"
        throw new RuntimeException(msg)
    }
}

trait StrategyProvider {

  /**
   * Returns details on a potential strategy if the filter is valid for this strategy.
   *
   * @param filter
   * @param sft
   * @return
   */
  def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints): Option[StrategyDecision]
}

case class StrategyDecision(strategy: Strategy, cost: Long)

case class PropertyLiteral(name: String, literal: Literal, secondary: Option[Literal], flipped: Boolean = false)
