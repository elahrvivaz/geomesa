/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

import java.util.concurrent.Executors

import com.github.benmanes.caffeine.cache.Ticker
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.index.planning.{InMemoryQueryRunner, QueryRunner}
import org.locationtech.geomesa.index.stats.NoopStats
import org.locationtech.geomesa.kafka.index.{FeatureCacheGuava, KafkaFeatureCache}
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration
import scala.io.Source

object LoadTest extends App {

  private val sft: SimpleFeatureType = CLArgResolver.getSft("gdelt")

  private val filter: Filter = WithClose(Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("urban100.url"))) { source =>
    ECQL.toFilter(source.getLines.next)
  }

  private val runner: QueryRunner = {
    val index: KafkaFeatureCache = new FeatureCacheGuava(sft, Duration.Inf)(Ticker.systemTicker())

    val file = "/opt/devel/src/geomesa/geomesa-kafka/geomesa-kafka-tools/src/test/resources/20160901.export.CSV.gz"
    val features = PathUtils.interpretPath(file).flatMap { file =>
      val converter = SimpleFeatureConverters.build(sft, CLArgResolver.getConfig("gdelt"))
      val ec = converter.createEvaluationContext(Map("inputFilePath" -> file.path))
      val is = PathUtils.handleCompression(file.open, file.path)
      converter.process(is, ec)
    }
    features.take(20000).foreach(index.put)
    println(s"loaded ${index.size()} features")

    new InMemoryQueryRunner(NoopStats, None) {
      override protected def name: String = "Test"
      override protected def features(sft: SimpleFeatureType, filter: Option[Filter]): CloseableIterator[SimpleFeature] =
        index.query(filter.getOrElse(Filter.INCLUDE))
    }
  }

  class TestRunner(i: Int) extends Runnable with MethodProfiling {
    override def run(): Unit = {
      implicit val timing: Timing = new Timing
      profile { runner.runQuery(sft, new Query(sft.getTypeName, filter)).length }
      profile { runner.runQuery(sft, new Query(sft.getTypeName, filter)).length }
      profile { runner.runQuery(sft, new Query(sft.getTypeName, filter)).length }
      println(s"$i ${timing.time}ms")
    }
  }


  val numThreads = Seq(1, 2, 4, 8)
  val maxThreads = numThreads.max
  val runners = (0 until maxThreads).map(new TestRunner(_))
  val es = Executors.newFixedThreadPool(maxThreads)
  println("warmup")
  runners.map(es.submit).foreach(_.get)
  println

  println("waiting")
  System.in.read()
  println("resuming\n")

  numThreads.foreach { nt =>
    println(s"$nt threads:")
    val threads = runners.take(nt).map(es.submit)
    Thread.sleep(500)
    val start = System.currentTimeMillis()
    runner.runQuery(sft, new Query(sft.getTypeName, Filter.INCLUDE)).length
    val time = System.currentTimeMillis() - start
    threads.foreach(_.get)
    println(s"include: ${time}ms")
    println
  }

  es.shutdown()
}
