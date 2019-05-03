/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.specification.core.Fragments

/**
  * Trait to simplify data store tests that use a single simple feature type
  *
  * Classes extending this trait should be named 'fooIT.scala' so that they are run during the
  * integration-test phase in maven, which will ensure that a single mini cluster is shared between all the tests
  */
abstract class TestWithFeatureType extends TestWithDataStore {

  def spec: String

  // we use class name to prevent spillage between unit tests in the mock connector
  lazy val sftName: String = getClass.getSimpleName

  lazy val sft = {
    ds.createSchema(SimpleFeatureTypes.createType(sftName, spec))
    ds.getSchema(sftName) // reload the sft from the ds to ensure all user data is set properly
  }

  // after all tests, drop the tables we created to free up memory
  override def map(fragments: => Fragments): Fragments =
    super.map(fragments ^ fragmentFactory.step(ds.removeSchema(sftName)))
}
