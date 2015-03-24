/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.analytics

import java.util.regex.Pattern

import com.twitter.algebird.Aggregator
import com.twitter.scalding._
import com.twitter.scalding.typed.UnsortedGrouped
import org.apache.accumulo.core.data.{Range => AcRange}
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.jobs.GeoMesaBaseJob
import org.locationtech.geomesa.jobs.analytics.HistogramJob._
import org.locationtech.geomesa.jobs.scalding.ConnectionParams._
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

class HistogramJob(args: Args) extends GeoMesaBaseJob(args) {

  val feature  = args(FEATURE_IN)
  val dsParams = toDataStoreInParams(args)
  val filter   = args.optional(CQL_IN)

  val attribute  = args(ATTRIBUTE)
  val groupBy    = args.optional(GROUP_BY)
  val valueRegex = try { args.optional(VALUE_REGEX).map(_.r.pattern) } catch {
    case e: Exception => throw new IllegalArgumentException(s"Invalid regex ${args(VALUE_REGEX)}", e)
  }
  val transform  = Some(Array(Some(attribute), groupBy, valueRegex).flatten)

  // TODO fix transforms
  val input = GeoMesaInputOptions(dsParams, feature, filter/*, transform*/)
  val output = args(FILE_OUT)

  // verify input params - inside a block so they don't get serialized
  {
    val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]
    val sft = ds.getSchema(feature)
    assert(sft != null, s"The feature '$feature' does not exist in the input data store")
    val descriptors = sft.getAttributeDescriptors.map(_.getLocalName)
    assert(descriptors.contains(attribute),  s"Attribute '$attribute' does not exist in feature $feature")
    groupBy.foreach { a =>
      assert(descriptors.contains(a),  s"Attribute '$a' does not exist in feature $feature")
    }
  }

  def regexFilter(sf: SimpleFeature, p: Pattern): Boolean =
    Option(sf.getAttribute(attribute)).exists(v => p.matcher(v.toString).matches())

  def groupByAttributes(sf: SimpleFeature): (String, String) = {
    val group = groupBy.flatMap(a => Option(sf.getAttribute(a)).map(_.toString)).getOrElse("")
    val attr = Option(sf.getAttribute(attribute)).map(_.toString).getOrElse("null")
    (group, attr)
  }

  val features: TypedPipe[SimpleFeature] = TypedPipe.from(GeoMesaSource(input)).values
  val filtered: TypedPipe[SimpleFeature] = valueRegex match {
    case Some(p) => features.filter(sf => regexFilter(sf, p))
    case None    => features
  }
  val groups: Grouped[(String, String), SimpleFeature] = filtered.groupBy(groupByAttributes)

  // in scalding 0.13 can do: groups.aggregate(Aggregator.size)
  implicit val prep = (_: Any) => 1L
  val aggregates: UnsortedGrouped[(String, String), Long] = groups.aggregate(Aggregator.fromMonoid[Any, Long])

  if (groupBy.isDefined) {
    aggregates.toTypedPipe.map { case ((group, attribute), count) => (group, attribute, count) }
        .write(TypedTsv[(String, String, Long)](output))
  } else {
    aggregates.toTypedPipe.map { case ((_, attribute), count) => (attribute, count) }
        .write(TypedTsv[(String, Long)](output))
  }
}

object HistogramJob {

  // select distinct icao group by airfield

  val ATTRIBUTE   = "geomesa.hist.attribute"
  val GROUP_BY    = "geomesa.hist.group.attribute"
  val VALUE_REGEX = "geomesa.hist.value.regex"
  val FILE_OUT    = "geomesa.hist.file.out"

  def runJob(conf: Configuration,
             dsParams: Map[String, String],
             feature: String,
             attribute: String,
             groupBy: Option[String],
             valueRegex: Option[String]) = {
    val args = Seq(FEATURE_IN -> List(feature),
                   ATTRIBUTE  -> List(attribute),
                   GROUP_BY   -> groupBy.toList,
                   VALUE_REGEX      -> valueRegex.toList).toMap ++ toInArgs(dsParams)
    val instantiateJob = (args: Args) => new HistogramJob(args)
    GeoMesaBaseJob.runJob(conf, args, instantiateJob)
  }
}
