/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.filter

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Coordinate
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.{TestWithDataStore, TestWithFeatureType}
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class FilterIT extends TestWithFeatureType {

  override val spec = SimpleFeatureTypes.encodeType(TestData.featureType, includeUserData = true)

  val mediumDataFeatures: Seq[SimpleFeature] =
    TestData.mediumData.map(TestData.createSF).map(f => new ScalaSimpleFeature(sft, f.getID, f.getAttributes.toArray))

  step {
    addFeatures(mediumDataFeatures)
  }

  "Filters" should {
    "filter correctly for all predicates" >> {
      runTest(goodSpatialPredicates)
    }

    "filter correctly for AND geom predicates" >> {
      runTest(andedSpatialPredicates)
    }

    "filter correctly for OR geom predicates" >> {
      runTest(oredSpatialPredicates)
    }

    "filter correctly for OR geom predicates with projections" >> {
      runTest(oredSpatialPredicates, Array("geom"))
    }

    "filter correctly for basic temporal predicates" >> {
      runTest(temporalPredicates)
    }

    "filter correctly for basic spatiotemporal predicates" >> {
      runTest(spatioTemporalPredicates)
    }

    "filter correctly for basic spariotemporal predicates with namespaces" >> {
      runTest(spatioTemporalPredicatesWithNS)
    }

    "filter correctly for attribute predicates" >> {
      runTest(attributePredicates)
    }

    "filter correctly for attribute and geometric predicates" >> {
      runTest(attributeAndGeometricPredicates)
    }

    "filter correctly for attribute and geometric predicates with namespaces" >> {
      runTest(attributeAndGeometricPredicatesWithNS)
    }

    "filter correctly for DWITHIN predicates" >> {
      runTest(dwithinPointPredicates)
    }.pendingUntilFixed("we are handling these correctly and geotools is not (probably)")

    "filter correctly for ID predicates" >> {
      runTest(idPredicates)
    }
  }

  def compareFilter(filter: Filter, projection: Array[String]) = {
    val filterCount = mediumDataFeatures.count(filter.evaluate)
    val query = new Query(sftName, filter)
    Option(projection).foreach(query.setPropertyNames)
    val queryCount = runQuery(query).length
    logger.debug(s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${mediumDataFeatures.size}: " +
        s"filter hits: $filterCount query hits: $queryCount")
    queryCount mustEqual filterCount
  }

  def runTest(filters: Seq[String], projection: Array[String] = null) =
    forall(filters.map(ECQL.toFilter))(compareFilter(_, projection))
}
