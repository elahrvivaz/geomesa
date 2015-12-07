/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class BinEncoder(sft: SimpleFeatureType, trackIdField: String) {

  val trackIdIndex = sft.indexOf(trackIdField)
  val geomIndex = sft.getGeomIndex

  val getLatLon: (SimpleFeature) => (Float, Float) = if (sft.isPoints) {
    (sf: SimpleFeature) => {
      val geom = sf.getAttribute(geomIndex).asInstanceOf[Point]
      (geom.getY.toFloat, geom.getX.toFloat)
    }
  } else {
    (sf: SimpleFeature) => {
      val geom = sf.getAttribute(geomIndex).asInstanceOf[Geometry].getCentroid
      (geom.getY.toFloat, geom.getX.toFloat)
    }
  }

  val getDtg: (SimpleFeature) => Long = sft.getDtgIndex match {
    case Some(i) => (sf: SimpleFeature) => {
      val dtg = sf.getAttribute(i).asInstanceOf[Date]
      if (dtg == null) 0L else dtg.getTime
    }
    case None => (sf: SimpleFeature) => 0L
  }

  def encode(sf: SimpleFeature): Array[Byte] = {
    val (lat, lon) = getLatLon(sf)
    val dtg = getDtg(sf)
    val trackIdVal = sf.getAttribute(trackIdIndex)
    val trackId = if (trackIdVal == null) "" else trackIdVal.toString
    Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
  }
}

object BinEncoder {
  def apply(sft: SimpleFeatureType): Option[BinEncoder] = sft.getBinTrackId.map(new BinEncoder(sft, _))
}