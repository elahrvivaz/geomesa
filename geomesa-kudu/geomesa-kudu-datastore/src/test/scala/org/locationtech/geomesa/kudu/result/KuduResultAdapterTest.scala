/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.result

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KuduResultAdapterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  "KuduResultAdapter" should {
    "serialize adapters" in {
      foreach(Seq(null, Array("dtg", "geom"))) { transform =>
        foreach(Seq(None, Some(ECQL.toFilter("name = 'foo'")))) { ecql =>
          foreach(Seq(Seq.empty, Seq("user".getBytes, "admin".getBytes))) { auths =>
            val query = new Query("test", ecql.getOrElse(Filter.INCLUDE), transform)
            QueryPlanner.setQueryTransforms(query, sft)
            val adapter = KuduResultAdapter(sft, auths, ecql, query.getHints)
            KuduResultAdapter.deserialize(KuduResultAdapter.serialize(adapter)).toString mustEqual adapter.toString
          }
        }
      }
    }
  }
}
