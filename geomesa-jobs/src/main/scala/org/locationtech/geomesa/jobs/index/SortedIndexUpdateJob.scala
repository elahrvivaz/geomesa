/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.jobs.index

import java.util

import com.twitter.scalding._
import org.apache.accumulo.core.data.{Key, Mutation, Range => AcRange, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

class SortedIndexUpdateJob(args: Args) extends GeoMesaBaseJob(args) {

  lazy val (stIndexTable, ranges) = {
    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    val sft: SimpleFeatureType = ds.getSchema(feature)
    val indexSchemaFmt = ds.getIndexSchemaFmt(sft.getTypeName)
    val encoding = ds.getFeatureEncoding(sft)
    val fe = SimpleFeatureEncoder(sft, encoding)
    val indexSchema = IndexSchema(indexSchemaFmt, sft, fe)
    val prefixes = (0 to indexSchema.maxShard).map {i =>
      indexSchema.encoder.rowf match { case CompositeTextFormatter(formatters, sep) =>
        formatters.take(2).map {
          case f: PartitionTextFormatter => f.fmt(i)
          case f: ConstantTextFormatter => f.constStr
        }.mkString("", sep, sep)
      }
    }
    val ranges = prefixes.map(p => new AcRange(p, p + "~"))
    (ds.getSpatioTemporalIdxTableName(feature), SerializedRange(ranges))
  }

  override lazy val input  = AccumuloInputOptions(stIndexTable, ranges)
  override lazy val output = AccumuloOutputOptions(stIndexTable)

  // scalding job
  AccumuloSource(options)
    .using(new SortedIndexUpdateResources)
    .flatMap(('key, 'value) -> 'mutation) { (r: SortedIndexUpdateResources, kv: (Key, Value)) =>
      val key = kv._1
      if (key.getColumnQualifier.getBytes.endsWith(SpatioTemporalTable.DATA_CQ_SUFFIX) ||
          key.getColumnQualifier.getBytes.endsWith(SpatioTemporalTable.INDEX_CQ_SUFFIX)) {
        Seq.empty
      } else {
        val value = kv._2
        val visibility = key.getColumnVisibilityParsed
        val mutation = new Mutation(key.getRow)
        mutation.putDelete(key.getColumnFamily, key.getColumnQualifier, visibility)
        if (key.getColumnQualifier.toString == "SimpleFeatureAttribute") {
          // data entry, we need to figure out the new key
          // we want to put it next to the index entry
          val sf = r.decoder.decode(value)
          val newKeys = r.encoder.encode(sf, visibility.toString)
              .map(_._1).filter(_.getColumnQualifier.getBytes.endsWith(SpatioTemporalTable.DATA_CQ_SUFFIX))
          newKeys.foreach(k => mutation.put(k.getColumnFamily, k.getColumnQualifier, visibility, value))
        } else {
          // index entry, just update the cf
          val cq = new Text(key.getColumnQualifier.copyBytes() ++ SpatioTemporalTable.INDEX_CQ_SUFFIX)
          mutation.put(key.getColumnFamily, cq, visibility, value)
        }
        Seq(mutation)
      }
    }.write(AccumuloSource(options))

  // override the run method to schedule a table compaction after the job finishes
  override def run: Boolean = {
    val result = super.run
    if (result) {
      // schedule a table compaction to remove the deleted entries
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
      ds.connector.tableOperations().compact(stIndexTable, null, null, true, false)
    }
    result
  }

  class SortedIndexUpdateResources extends GeoMesaResources {
    val indexSchemaFmt = ds.getIndexSchemaFmt(sft.getTypeName)
    val encoding = ds.getFeatureEncoding(sft)
    val fe = SimpleFeatureEncoder(sft, encoding)
    val encoder = IndexSchema.buildKeyEncoder(indexSchemaFmt, fe)
    val decoder = SimpleFeatureDecoder(sft, encoding)
  }
}

object SortedIndexUpdateJob {

  def runJob(conf: Configuration, params: Map[String, String], feature: String) = {

    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]

    require(ds != null, "Data store could not be loaded")

    val sft = ds.getSchema(feature)
    require(sft != null, s"Feature '$feature' does not exist")

    // create args to pass to scalding job based on our input parameters
    val args = buildArgs(params.asJava, feature)

    // set libjars so that our dependent libs get propagated to the cluster
    JobUtils.setLibJars(conf)

    // run the scalding job on HDFS
    val hdfsMode = Hdfs(strict = true, conf)
    val arguments = Mode.putMode(hdfsMode, args)

    val job = new SortedIndexUpdateJob(arguments)
    val flow = job.buildFlow
    flow.complete() // this blocks until the job is done
  }

  def buildArgs(params: util.Map[String, String], feature: String): Args =
    GeoMesaBaseJob.buildBaseArgs(params, feature)
}
