/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KuduResultAdapterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  "KuduResultAdapter" should {
    "serialize adapters" in {
      foreach(Seq(None, Some(("dtg=dtg;geom=geom", DataUtilities.createSubType(sft, Array("dtg", "geom")))))) { transform =>
        foreach(Seq(None, Some(ECQL.toFilter("name = 'foo'")))) { ecql =>
          foreach(Seq(Seq.empty, Seq("user".getBytes, "admin".getBytes))) { auths =>
            val adapter = KuduResultAdapter(sft, ecql, transform, auths)
            KuduResultAdapter.deserialize(KuduResultAdapter.serialize(adapter)) mustEqual adapter
          }
        }
      }
    }
  }
}
