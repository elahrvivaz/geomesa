/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, HBaseCatalogParam}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.view.{MergedDataStoreView, MergedDataStoreViewFactory}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseMergedDataStoreViewTest extends Specification {

  import scala.collection.JavaConverters._

  // note: h2 seems to require ints as the primary key, and then prepends `<typeName>.` when returning them
  // as such, we don't compare primary keys directly here
  // there may be a way to override this behavior but I haven't found it...

  sequential // note: shouldn't need to be sequential, but h2 doesn't do well with concurrent requests

  // we use class name to prevent spillage between unit tests in the mock connector
  val sftName: String = getClass.getSimpleName
  val spec = "name:String:index=full,age:Int,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType(sftName, spec)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", 20 + i, s"2018-01-01T00:0$i:00.000Z", s"POINT (45 5$i)")
  }

  val defaultFilter = ECQL.toFilter("bbox(geom,44,52,46,59) and dtg DURING 2018-01-01T00:02:30.000Z/2018-01-01T00:06:30.000Z")

  //implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)
  implicit val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  val params = Map(
    ConnectionParam.getName -> MiniCluster.connection,
    HBaseCatalogParam.getName -> getClass.getSimpleName
  )

  var ds: MergedDataStoreView = _

  def comboParams(params: java.util.Map[String, String]*): java.util.Map[String, String] = {
    val configs = params.map(ConfigValueFactory.fromMap).asJava
    val config = ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(configs))
    Map(MergedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())).asJava
  }

  step {

    val hbaseDS1 = DataStoreFinder.getDataStore(params.asJava)
    val hbaseDS2 = DataStoreFinder.getDataStore(params.asJava)

    val copied = features.iterator
    Seq(hbaseDS1, hbaseDS2).foreach { ds =>
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
        copied.take(5).foreach { copy =>
          FeatureUtils.copyToWriter(writer, copy, useProvidedFid = true)
          writer.write()
        }
      }
    }

    foreach(Seq(hbaseDS1, hbaseDS2)) { ds =>
      SelfClosingIterator(ds.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList must haveLength(10)
    }
    ds = new MergedDataStoreView(Seq(hbaseDS1 -> None, hbaseDS2 -> None))
  }

  "MergedDataStoreView" should {

    "query multiple data stores and return arrow" in {
      val query = new Query(sftName, defaultFilter, Array("name", "dtg", "geom"))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        val expected = features.slice(3, 7).zip(features.slice(3, 7)).flatMap { case (one, two) => Seq(one, two) }
        reader.dictionaries.keySet mustEqual Set("name")
        reader.dictionaries.apply("name").iterator.toSeq must containAllOf(expected.map(_.getAttribute("name")).distinct)
        val results = SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toList
        results must haveLength(expected.length)
        foreach(results.zip(expected)) { case (actual, e) =>
          actual.getAttributeCount mustEqual 3
          foreach(Seq("name", "dtg", "geom")) { attribute =>
            actual.getAttribute(attribute) mustEqual e.getAttribute(attribute)
          }
        }
      }
    }
  }

  step {
    ds.dispose()
    allocator.close()
  }
}

case object MiniCluster extends LazyLogging {

  lazy val cluster: HBaseTestingUtility = {
    val cluster = new HBaseTestingUtility()
    logger.info("Starting embedded hbase")
    cluster.getConfiguration.set("hbase.superuser", "admin")
    cluster.getConfiguration.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, classOf[GeoMesaCoprocessor].getName)
    cluster.startMiniCluster(sys.props.get("geomesa.hbase.test.servers").map(_.toInt).getOrElse(3))
    logger.info("Started embedded hbase")
    cluster
  }

  lazy val connection: Connection = cluster.getConnection

  sys.addShutdownHook({
    logger.info("Stopping embedded hbase")
    // note: HBaseTestingUtility says don't close the connection
    // connection.close()
    cluster.shutdownMiniCluster()
    logger.info("Embedded HBase stopped")
  })
}
