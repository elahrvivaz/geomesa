/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.BatchWriterConfig
import org.locationtech.geomesa.accumulo.data.MiniCluster
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloQueryEventTransformTest extends Specification {

  lazy val client = MiniCluster.client

  "AccumuloQueryEventTransform" should {
    "Convert from and to mutations" in {
      val event = QueryEvent(
        AccumuloAuditService.StoreType, // note: this isn't actually stored
        "type-name",
        System.currentTimeMillis(),
        "user",
        "filter",
        "hints",
        Long.MaxValue - 100,
        Long.MaxValue - 200,
        Long.MaxValue - 300,
        deleted = true
      )

      client.tableOperations().create("AccumuloQueryEventTransformTest")

      WithClose(client.createBatchWriter("AccumuloQueryEventTransformTest", new BatchWriterConfig())) { writer =>
        writer.addMutation(AccumuloQueryEventTransform.toMutation(event))
      }
      val restored = WithClose(client.createScanner("AccumuloQueryEventTransformTest", new Authorizations)) { reader =>
        import scala.collection.JavaConversions._
        AccumuloQueryEventTransform.toEvent(reader)
      }

      restored mustEqual event
    }
  }
}
