/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.security.Authorizations
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMiniCluster
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloQueryEventTransformIT extends TestWithMiniCluster {

  import scala.collection.JavaConverters._

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

      val table = "AccumuloQueryEventTransformTest"

      connector.tableOperations().create(table)

      WithClose(connector.createBatchWriter(table, new BatchWriterConfig())) { writer =>
        writer.addMutation(AccumuloQueryEventTransform.toMutation(event))
      }
      val restored = WithClose(connector.createScanner(table, new Authorizations)) { reader =>
        AccumuloQueryEventTransform.toEvent(reader.asScala)
      }

      restored mustEqual event
    }
  }
}
