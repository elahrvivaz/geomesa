/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.curve

import org.joda.time.{DateTime, Seconds, Weeks}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class NormalizedDimensionTest extends Specification {

  val rand = new Random(-574)

  def next: Double = rand.nextDouble() * 360.0 - 180.0

  val dim = Z2SFC.lon

  "NormalizedDimension" should {
    "norm/denorm" >> {
//      dim.denormalize(dim.normalize(0.0)) mustEqual 0.0
      dim.denormalize(dim.normalize(-180.0)) mustEqual -180.0
      dim.denormalize(dim.normalize(180.0)) mustEqual 180.0
      dim.denormalize(dim.normalize(10.5)) mustEqual 10.5
    }
  }
}
