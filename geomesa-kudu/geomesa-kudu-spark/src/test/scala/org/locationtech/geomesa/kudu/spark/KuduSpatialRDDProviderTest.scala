/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.spark

class KuduSpatialRDDProviderTest {

  import org.apache.spark.sql._

  val params = Map("kudu.master" -> "cluster-master.ccri.com","kudu.catalog" -> "geomesa", "geomesa.security.auths" -> "admin")

  val session = SparkSession.builder().appName("testSpark").master("local[*]").getOrCreate()

  val df = session.read.format("geomesa").options(params).option("geomesa.feature", "testpoints").load()

  df.createOrReplaceTempView("test")

  session.sql("select * from test").show
}
