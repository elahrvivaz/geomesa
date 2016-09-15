/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.kryo.json

import com.esotericsoftware.kryo.io.{Input, Output}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KryoJsonSerializationTest extends Specification {

  val geoms = Seq(
    """{ "type": "Point", "coordinates": [30, 10] }""",
    """{ "type": "LineString", "coordinates": [[30, 10], [10, 30], [40, 40]] }""",
    """{ "type": "Polygon", "coordinates": [[[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]] }""",
    """{ "type": "Polygon", "coordinates": [
      |    [[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]],
      |    [[20, 30], [35, 35], [30, 20], [20, 30]]
      |  ]
      |}""".stripMargin,
    """{ "type": "MultiPoint", "coordinates": [[10, 40], [40, 30], [20, 20], [30, 10]] }""",
    """{ "type": "MultiLineString", "coordinates": [
      |    [[10, 10], [20, 20], [10, 40]],
      |    [[40, 40], [30, 30], [40, 20], [30, 10]]
      |  ]
      |}
    """.stripMargin,
    """{ "type": "MultiPolygon", "coordinates": [
      |        [[[30, 20], [45, 40], [10, 40], [30, 20]]],
      |        [[[15, 5], [40, 10], [10, 20], [5, 10], [15, 5]]]
      |      ]
      |    }
    """.stripMargin,
    """{ "type": "MultiPolygon", "coordinates": [
      |    [[[40, 40], [20, 45], [45, 30], [40, 40]]],
      |    [[[20, 35], [10, 30], [10, 10], [30, 5], [45, 20], [20, 35]],[[30, 20], [20, 15], [20, 25], [30, 20]]]
      |  ]
      |}
    """.stripMargin
  )

  "KryoJsonSerialization" should {
    "correctly de/serialize basic json" in {
      val out = new Output(128)
      val json = """{ "foo" : false, "bar" : "yes", "baz" : [ 1, 2, 3 ] }"""
      KryoJsonSerialization.serialize(out, json)
      val bytes = out.toBytes
      bytes must not(beEmpty)
      val recovered = KryoJsonSerialization.deserialize(new Input(bytes))
      recovered mustEqual json.replaceAll(" ", "")
    }
    "correctly de/serialize geojson" in {
      val out = new Output(512)
      val jsons = geoms.map { geom =>
        s"""{ "type": "Feature", "geometry": $geom, "properties": { "prop0": "value0", "prop1": { "this": "that" } } }"""
      }
      forall(jsons) { json =>
        out.clear()
        KryoJsonSerialization.serialize(out, json)
        val bytes = out.toBytes
        bytes must not(beEmpty)
        val recovered = KryoJsonSerialization.deserialize(new Input(bytes))
        recovered mustEqual json.replaceAll("[ \n]", "")
      }
    }
  }
}
