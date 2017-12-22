/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.File
import java.nio.charset.StandardCharsets

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, JobStatus, Mapper}
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.locationtech.geomesa.jobs.mapreduce.{ConverterInputFormat, GeoMesaOutputFormat}
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobUtils}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

abstract class BulkIngestConverterJob[T, V](sft: SimpleFeatureType, converterConfig: Config)
                                           (implicit tt: ClassTag[T], vt: ClassTag[V]) {

  import ConverterInputFormat.{Counters => ConvertCounters}
  import GeoMesaOutputFormat.{Counters => OutCounters}

  private val failCounters =
    Seq((ConvertCounters.Group, ConvertCounters.Failed), (OutCounters.Group, OutCounters.Failed))

  def mapper: Class[_ <: Mapper[LongWritable, SimpleFeature, T, V]]

  /**
    * Set output format, configuration options, output format
    *
    * @param job job
    */
  def configureOutput(job: Job, output: String): Unit/* = {
    job.setOutputFormatClass(classOf[GeoMesaOutputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[ScalaSimpleFeature])
  }*/

  def notifyBulkLoad(output: String): Unit

  def converted(job: Job): Long = job.getCounters.findCounter(OutCounters.Group, OutCounters.Written).getValue
  // TODO
  def written(job: Job): Long = job.getCounters.findCounter(OutCounters.Group, OutCounters.Written).getValue
  def failed(job: Job): Long = failCounters.map(c => job.getCounters.findCounter(c._1, c._2).getValue).sum

  def run(dsParams: Map[String, String],
          typeName: String,
          paths: Seq[String],
          reducers: Int,
          output: String,
          tempPath: Option[String],
          libjarsFile: String,
          libjarsPaths: Iterator[() => Seq[File]],
          statusCallback: StatusCallback): (Long, Long) = {

    import scala.collection.JavaConversions._

    val job = Job.getInstance(new Configuration, "GeoMesa Bulk Ingest")

    JobUtils.setLibJars(job.getConfiguration, readLibJars(libjarsFile), defaultSearchPath ++ libjarsPaths)

    job.setJarByClass(getClass)
    job.setInputFormatClass(classOf[ConverterInputFormat])
    job.setMapperClass(mapper)
    job.setMapOutputKeyClass(tt.runtimeClass)
    job.setMapOutputValueClass(vt.runtimeClass)
    job.setNumReduceTasks(reducers)
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")

    FileInputFormat.setInputPaths(job, paths.mkString(","))
    ConverterInputFormat.setConverterConfig(job, converterConfig.root().render(ConfigRenderOptions.concise()))
    ConverterInputFormat.setSft(job, sft)

    GeoMesaConfigurator.setFeatureTypeOut(job.getConfiguration, typeName)
    GeoMesaOutputFormat.configureDataStore(job, dsParams)

    configureOutput(job, tempPath.getOrElse(output))

    Command.user.info("Submitting job - please wait...")
    job.submit()
    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    def mapCounters = Seq(("converted", converted(job)), ("failed", failed(job)))
    def reduceCounters = Seq(("written", written(job)))

    val stages = if (tempPath.isDefined) { 3 } else { 2 }

    var mapping = true
    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        if (mapping) {
          val mapProgress = job.mapProgress()
          if (mapProgress < 1f) {
            statusCallback(s"Map (stage 1/$stages): ", mapProgress, mapCounters, done = false)
          } else {
            statusCallback(s"Map (stage 1/$stages): ", mapProgress, mapCounters, done = true)
            statusCallback.reset()
            mapping = false
          }
        } else {
          statusCallback(s"Reduce (stage 2/$stages): ", job.reduceProgress(), reduceCounters, done = false)
        }
      }
      Thread.sleep(500)
    }
    statusCallback(s"Reduce (stage 2/$stages): ", job.reduceProgress(), reduceCounters, done = true)

    // Do this earlier than the data copy bc its throwing errors
    val result = (converted(job), failed(job))

    if (!job.isSuccessful) {
      // TODO track failure in rest of messages...
      Command.user.error(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    } else {
      val copied = tempPath.forall { path =>
        // TODO merge with fs code
        val typeName = sft.getTypeName
        statusCallback.reset()

        Command.user.info("Submitting DistCp job - please wait...")
        val opts = new DistCpOptions(List(new Path(path)), new Path(output))
        opts.setAppend(false)
        opts.setOverwrite(true)
        opts.setCopyStrategy("dynamic")
        val job = new DistCp(new Configuration, opts).execute()

        Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

        // distCp has no reduce phase
        while (!job.isComplete) {
          if (job.getStatus.getState != JobStatus.State.PREP) {
            statusCallback(s"DistCp (stage $stages/$stages): ", job.mapProgress(), Seq.empty, done = false)
          }
          Thread.sleep(500)
        }
        statusCallback(s"DistCp (stage $stages/$stages): ", job.mapProgress(), Seq.empty, done = true)

        val success = job.isSuccessful
        if (success) {
          Command.user.info(s"Successfully copied data to $output")
        } else {
          Command.user.error(s"Failed to copy data to $output")
        }
        success
      }

      if (copied) {
        Command.user.info("Triggering bulk load")
        notifyBulkLoad(output)
      }
    }

    (written(job), failed(job))
  }

  protected def readLibJars(file: String): java.util.List[String] = {
    val is = getClass.getClassLoader.getResourceAsStream(file)
    try {
      IOUtils.readLines(is, StandardCharsets.UTF_8)
    } catch {
      case NonFatal(e) => throw new Exception("Error reading ingest libjars", e)
    } finally {
      IOUtils.closeQuietly(is)
    }
  }

  protected def defaultSearchPath: Iterator[() => Seq[File]] =
    Iterator(
      () => ClassPathUtils.getJarsFromClasspath(getClass),
      () => ClassPathUtils.getFilesFromSystemProperty("geomesa.convert.scripts.path")
    )
}
