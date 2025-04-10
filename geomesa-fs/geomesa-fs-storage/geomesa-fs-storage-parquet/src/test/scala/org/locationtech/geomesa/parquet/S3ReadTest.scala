/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet


import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, Metadata, NamedOptions}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory
import org.locationtech.geomesa.fs.storage.parquet.{ParquetFileSystemStorage, ParquetFileSystemStorageFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class S3ReadTest extends Specification with BeforeAfterAll with StrictLogging {

  sequential

  var minio: MinIOContainer = _
  val bucket = "geomesa"

  val scheme = NamedOptions("daily")

  val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

  val features = Seq.tabulate(10) { i =>
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf.setAttribute(0, s"POINT(4$i 5$i)")
    sf.setAttribute(1, s"name$i")
    sf.setAttribute(2, s"$i")
    sf.setAttribute(3, f"2014-01-01T$i%02d:00:01.000Z")
    sf
  }

  override def beforeAll(): Unit = {
    minio =
      new MinIOContainer(
        DockerImageName.parse("minio/minio").withTag(sys.props.getOrElse("minio.docker.tag", "RELEASE.2024-10-29T16-01-48Z")))
    minio.start()
    minio.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("minio")))
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", s"localhost/$bucket")
  }

  override def afterAll(): Unit = {
    if (minio != null) {
      minio.close()
    }
  }

  private def prop(key: String, value: String): String = s"<property><name>$key</name><value>$value</value></property>"

  "ParquetFileSystemStorage" should {
    "use vectored IO to improve s3 read speeds" in {
      def storage(useVectoredIo: Boolean): ParquetFileSystemStorage = {
        val bucket = s"s3a://${this.bucket}/"
        val props = Seq(
          prop("fs.s3a.endpoint", minio.getS3URL),
          prop("fs.s3a.access.key", minio.getUserName),
          prop("fs.s3a.secret.key", minio.getPassword),
          prop("fs.s3a.path.style.access", "true"),
          prop("dfs.client.use.datanode.hostname", "true"),
          prop("parquet.compression", "gzip"),
          prop("parquet.hadoop.vectored.io.enabled", useVectoredIo.toString),
        )
        val xml =
          s"""<configuration>
             |  ${props.mkString("\n  ")}
             |</configuration>
             |""".stripMargin

        val config = new Configuration()
        config.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))

        val context = FileSystemContext(new Path(bucket), config)
        val metadataFactory = new FileBasedMetadataFactory()
        val metadata =
          metadataFactory.load(context)
            .getOrElse(metadataFactory.create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true)))
        new ParquetFileSystemStorageFactory().apply(context, metadata)
      }

      WithClose(storage(false)) { storage =>
        // number of times to write the sample features into our file
        val multiplier = 17715 // 177156 * 10

        val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

        features.foreach { f =>
          val partition = storage.metadata.scheme.getPartitionName(f)
          val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
          var i = 0
          while (i < multiplier) {
            writer.write(f)
            i += 1
          }
        }

        writers.foreach(_._2.close())

        logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")
      }

      val results = Seq(true, false).flatMap { useVectoredIo =>
        WithClose(storage(useVectoredIo)) { storage =>
          def testQuery(filter: String, transforms: Array[String]): TestResult = {
            val query = new Query(sft.getTypeName, ECQL.toFilter(filter), transforms: _*)
            val start = System.currentTimeMillis()
            val iter = storage.getReader(query)
            val count =
              try { iter.length } finally {
                iter.close()
              }
            TestResult(filter, Option(transforms).getOrElse(Array.empty), count, System.currentTimeMillis() - start, useVectoredIo)
          }

          val filters = Seq(
            "INCLUDE",
            "IN('0', '2')",
            "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T08:00:00.000Z",
            "bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z",
            "bbox(geom,42,48,52,62)",
            "dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T08:00:00.000Z",
            "name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T08:00:00.000Z",
            "name < 'name5'",
            "name = 'name5'",
            "age < 5",
          )
          val transforms = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))

          filters.flatMap { filter =>
            transforms.map { transform =>
              testQuery(filter, transform)
            }
          }
        }
      }


      logger.debug(s"| Filter | Transform | Count | Time | Vectorized Time | Delta |")
      logger.debug(s"| -- | -- | -- | -- | -- | -- |")
      val merged = results.groupBy(r => (r.filter, r.transform)).values.map { toMerge =>
        toMerge.reduceLeft[TestResult] { case (a, b) =>
          a.copy(time = math.max(a.time, b.time), vectorizedTime = math.max(a.vectorizedTime, b.vectorizedTime))
        }
      }
      merged.map(_.toString).toList.sorted.foreach(m => logger.debug(m))

      ok
    }
  }

  case class TestResult(filter: String, transform: String, count: Int, time: Long, vectorizedTime: Long) {
    override def toString: String =
      s"| $filter | $transform | $count | ${time}ms | ${vectorizedTime}ms | ${vectorizedTime - time}ms |"
  }

  object TestResult {
    def apply(filter: String, transform: Array[String], count: Int, time: Long, vectorized: Boolean): TestResult =
      TestResult(filter, Option(transform).getOrElse(Array.empty).mkString(","), count, if (vectorized) { 0L } else { time }, if (vectorized) { time } else { 0L })
  }
}
