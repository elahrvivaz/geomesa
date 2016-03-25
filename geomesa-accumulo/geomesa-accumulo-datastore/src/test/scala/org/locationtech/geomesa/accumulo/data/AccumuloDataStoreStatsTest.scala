/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data

import java.util.Date

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, wholeWorldEnvelope}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreStatsTest extends Specification with TestWithDataStore {

  sequential

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  val filter = Filter.INCLUDE

  val baseMillis = {
    val sf = new ScalaSimpleFeature("", sft)
    sf.setAttribute(1, "2016-01-02T00:00:00.000Z")
    sf.getAttribute(1).asInstanceOf[Date].getTime
  }

  val dayInMillis = new DateTime(baseMillis, DateTimeZone.UTC).plusDays(1).getMillis - baseMillis

  "AccumuloDataStore" should {
    "track stats for ingested features" >> {

      "initially have global stats" >> {
        ds.stats.getBounds(sft, filter) mustEqual wholeWorldEnvelope
        val initialTimeBounds = ds.stats.getMinMax[Date](sft, "dtg", filter)
        initialTimeBounds._1.getTime mustEqual Long.MinValue
        initialTimeBounds._2.getTime mustEqual Long.MaxValue
      }

      "through feature writer append" >> {
        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        val sf = writer.next()
        sf.setAttribute(1, "2016-01-02T00:00:00.000Z")
        sf.setAttribute(2, "POINT (0 0)")
        writer.write()
        writer.flush()

        ds.stats.getBounds(sft, filter) mustEqual new ReferencedEnvelope(0, 0, 0, 0, CRS_EPSG_4326)
        ds.stats.getMinMax[Date](sft, "dtg", filter) mustEqual
            (new Date(baseMillis), new Date(baseMillis + 1))

        val sf2 = writer.next()
        sf2.setAttribute(1, "2016-01-02T12:00:00.000Z")
        sf2.setAttribute(2, "POINT (10 10)")
        writer.write()
        writer.close()

        ds.stats.getBounds(sft, filter) mustEqual new ReferencedEnvelope(0, 10, 0, 10, CRS_EPSG_4326)
        ds.stats.getMinMax[Date](sft, "dtg", filter) mustEqual
            (new Date(baseMillis), new Date(baseMillis + (dayInMillis / 2) + 1))
      }

      "through feature source add features" >> {
        val fs = ds.getFeatureSource(sftName)

        val sf = new ScalaSimpleFeature("collection1", sft)
        sf.setAttribute(1, "2016-01-03T00:00:00.000Z")
        sf.setAttribute(2, "POINT (-10 -10)")

        val features = new DefaultFeatureCollection()
        features.add(sf)
        fs.addFeatures(features)

        ds.stats.getBounds(sft, filter) mustEqual new ReferencedEnvelope(-10, 10, -10, 10, CRS_EPSG_4326)
        ds.stats.getMinMax[Date](sft, "dtg", filter)  mustEqual
            (new Date(baseMillis), new Date(baseMillis + dayInMillis + 1))
      }

      "not expand bounds when not necessary" >> {
        val writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)

        val sf = writer.next()
        sf.setAttribute(1, "2016-01-02T00:00:00.000Z")
        sf.setAttribute(2, "POINT (0 0)")
        writer.write()
        writer.close()

        ds.stats.getBounds(sft, filter) mustEqual new ReferencedEnvelope(-10, 10, -10, 10, CRS_EPSG_4326)
        ds.stats.getMinMax[Date](sft, "dtg", filter) mustEqual
            (new Date(baseMillis), new Date(baseMillis + dayInMillis + 1))
      }

      "through feature source set features" >> {
        val fs = ds.getFeatureSource(sftName)

        val sf = new ScalaSimpleFeature("", sft)
        sf.setAttribute(1, "2016-01-01T00:00:00.000Z")
        sf.setAttribute(2, "POINT (15 0)")

        val features = new SimpleFeatureReader() {
          val iter = Iterator(sf)
          override def next(): SimpleFeature = iter.next()
          override def hasNext: Boolean = iter.hasNext
          override def getFeatureType: SimpleFeatureType = sft
          override def close(): Unit = {}
        }

        fs.setFeatures(features)

        ds.stats.getBounds(sft, filter) mustEqual new ReferencedEnvelope(-10, 15, -10, 10, CRS_EPSG_4326)
        ds.stats.getMinMax[Date](sft, "dtg", filter) mustEqual
            (new Date(baseMillis - dayInMillis), new Date(baseMillis + dayInMillis + 1))
      }
    }
  }
}
