/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.time

object TimePeriod extends Enumeration {

  type TimePeriod = Value

  val Day:   TimePeriod = Value("day")
  val Week:  TimePeriod = Value("week")
  val Month: TimePeriod = Value("month")
  val Year:  TimePeriod = Value("year")
}
