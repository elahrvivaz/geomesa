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

package org.locationtech.geomesa.core.index

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexValueEncoderTest extends Specification {

  val defaultSchema = "*geom:Geometry,dtg:Date,s:String,i:Int,d:Double,f:Float,u:UUID,l:List[String]"
  val id = "Feature0123456789"
  val geom = WKTUtils.read("POINT (-78.495356 38.075215)")
  val dt = new DateTime().toDate

  // b/c the IndexValueEncoder caches feature types, we need to change the sft name for each test
  val index = new AtomicInteger(0)
  def getSft(schema: String = defaultSchema) =
    SimpleFeatureTypes.createType("IndexValueEncoderTest" + index.getAndIncrement, schema)

  "IndexValueEncoder" should {
    "default to id,geom,date" in {
      val sft = getSft()
      core.index.setDtgDescriptor(sft, "dtg")
      IndexValueEncoder(sft).fields mustEqual Seq("id", "geom", "dtg")
    }
    "default to id,geom if no date" in {
      val sft = getSft("*geom:Geometry,foo:String")
      IndexValueEncoder(sft).fields mustEqual Seq("id", "geom")
    }
    "allow custom fields to be set" in {
      val sft = getSft()
      core.index.setIndexValueSchema(sft, "id,geom,dtg,i,f")
      IndexValueEncoder(sft).fields mustEqual Seq("id", "geom", "dtg", "i", "f")
    }
    "always include id,geom,dtg" in {
      val sft = getSft()
      core.index.setIndexValueSchema(sft, "foo,bar")
      IndexValueEncoder(sft) must throwA[IllegalArgumentException]
    }

    "encode and decode id,geom,date" in {
      val sft = getSft()
      core.index.setDtgDescriptor(sft, "dtg")

      // inputs
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded(0) mustEqual(id)
      decoded(1) mustEqual(geom)
      decoded(2) mustEqual(dt)
    }

    "encode and decode id,geom,date when there is no date" in {
      val sft = getSft()
      core.index.setDtgDescriptor(sft, "dtg")

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded(0) mustEqual(id)
      decoded(1) mustEqual(geom)
      decoded(2) must beNull
    }

    "encode and decode custom fields" in {
      val sft = getSft()
      core.index.setIndexValueSchema(sft, "id,geom,dtg,s,i,d,f,u")

      val s = "test"
      val i: java.lang.Integer = 5
      val d: java.lang.Double = 10d
      val f: java.lang.Float = 1.0f
      val u = UUID.randomUUID()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, s, i, d, f, u, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded(0) mustEqual id
      decoded(1) mustEqual geom
      decoded(2) mustEqual dt
      decoded(3) mustEqual s
      decoded(4) mustEqual i
      decoded(5) mustEqual d
      decoded(6) mustEqual f
      decoded(7) mustEqual u
    }

    "encode and decode null values" in {
      val sft = getSft()
      core.index.setIndexValueSchema(sft, "id,geom,dtg,s,i,d,f,u")

      val i: java.lang.Integer = 5
      val d: java.lang.Double = 10d
      val f: java.lang.Float = 1.0f

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, i, d, f, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.encode(entry)

      // requirements
      value must not beNull

      // return trip
      val decoded = encoder.decode(value)

      // requirements
      decoded must not beNull;
      decoded(0) mustEqual id
      decoded(1) mustEqual geom
      decoded(2) must beNull
      decoded(3) must beNull
      decoded(4) mustEqual i
      decoded(5) mustEqual d
      decoded(6) mustEqual f
      decoded(7) must beNull
    }

    "not allow complex types" in {
      val sft = getSft()
      core.index.setIndexValueSchema(sft, "id,geom,dtg,l")

      IndexValueEncoder(sft) must throwAn[IllegalArgumentException]
    }

    "be at least as fast as before" in {
      skipped("for integration")

      val sft = getSft()
      core.index.setDtgDescriptor(sft, "dtg")

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      var totalEncodeNew = 0L
      var totalDecodeNew = 0L

      var totalEncodeOld = 0L
      var totalDecodeOld = 0L

      // run once to remove any initialization time...
      IndexEntry.encodeIndexValue(entry)
      encoder.encode(entry)

      (0 to 1000000).foreach { _ =>
        val start = System.currentTimeMillis()
        val value = encoder.encode(entry)
        val encode = System.currentTimeMillis()
        encoder.decode(value)
        val decode = System.currentTimeMillis()

        totalEncodeNew += encode - start
        totalDecodeNew += decode - encode
      }

      (0 to 1000000).foreach { _ =>
        val start = System.currentTimeMillis()
        val value = IndexEntry.encodeIndexValue(entry)
        val encode = System.currentTimeMillis()
        IndexEntry.decodeIndexValue(value)
        val decode = System.currentTimeMillis()

        totalEncodeOld += encode - start
        totalDecodeOld += decode - encode
      }

      println(s"old $totalEncodeOld $totalDecodeOld")
      println(s"new $totalEncodeNew $totalDecodeNew")
      println
      success
    }
  }
}
