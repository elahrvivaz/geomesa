/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve.z

import org.locationtech.geomesa.curve.NormalizedDimension.{SemiNormalizedLat, SemiNormalizedLon, SemiNormalizedTime}
import org.locationtech.geomesa.curve.time.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.time.{BinnedTime, TimePeriod}

@deprecated("Z3SFC", "1.3.2")
class LegacyZ3SFC(period: TimePeriod) extends Z3SFC(period, 21) {
  override val dx = SemiNormalizedLon(math.pow(2, 21).toLong - 1)
  override val dy = SemiNormalizedLat(math.pow(2, 21).toLong - 1)
  override val dz = SemiNormalizedTime(math.pow(2, 20).toLong - 1, BinnedTime.maxOffset(period).toDouble)
}

@deprecated("Z3SFC", "1.3.2")
object LegacyZ3SFC {

  private val SfcDay   = new LegacyZ3SFC(TimePeriod.Day)
  private val SfcWeek  = new LegacyZ3SFC(TimePeriod.Week)
  private val SfcMonth = new LegacyZ3SFC(TimePeriod.Month)
  private val SfcYear  = new LegacyZ3SFC(TimePeriod.Year)

  def apply(period: TimePeriod): LegacyZ3SFC = period match {
    case TimePeriod.Day   => SfcDay
    case TimePeriod.Week  => SfcWeek
    case TimePeriod.Month => SfcMonth
    case TimePeriod.Year  => SfcYear
  }
}