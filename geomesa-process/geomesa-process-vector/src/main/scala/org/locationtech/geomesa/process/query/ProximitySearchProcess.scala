/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.process.query

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.GeometryFactory
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.process.{GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.geotools.Conversions.{RichGeometry, _}
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

@DescribeProcess(
  title = "Geomesa-enabled Proximity Search",
  description = "Performs a proximity search on a Geomesa feature collection using another feature collection as input"
)
class ProximitySearchProcess extends GeoMesaProcess with LazyLogging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "Input feature collection that defines the proximity search")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "The data set to query for matching features")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "bufferDistance",
                 description = "Buffer size in meters")
               bufferDistance: java.lang.Double

               ): SimpleFeatureCollection = {

    logger.debug(s"Attempting Geomesa Proximity Search on collection type ${dataFeatures.getClass.getName}")

    val visitor = new ProximityVisitor(inputFeatures, dataFeatures, bufferDistance)
    dataFeatures.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[ProximityResult].results
  }
}

class ProximityVisitor(inputFeatures: SimpleFeatureCollection,
                       dataFeatures: SimpleFeatureCollection,
                       bufferDistance: java.lang.Double)
    extends GeoMesaProcessVisitor with LazyLogging {

  val geoFac = new GeometryFactory
  val ff = CommonFactoryFinder.getFilterFactory2

  var manualFilter: Filter = _
  val manualVisitResults = new DefaultFeatureCollection(null, dataFeatures.getSchema)

  // Called for non AccumuloFeactureCollections - here we use degrees for our filters
  // since we are manually evaluating them.
  def visit(feature: Feature): Unit = {
    manualFilter = Option(manualFilter).getOrElse(dwithinFilters("degrees"))
    val sf = feature.asInstanceOf[SimpleFeature]

    if (manualFilter.evaluate(sf)) {
      manualVisitResults.add(sf)
    }
  }

  var resultCalc: ProximityResult = ProximityResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug("Running Geomesa Proximity Search on source type "+source.getClass.getName)
    val combinedFilter = ff.and(query.getFilter, dwithinFilters("meters"))
    resultCalc = ProximityResult(source.getFeatures(combinedFilter))
  }

  def dwithinFilters(requestedUnit: String): Filter = {
    val geomProperty = ff.property(dataFeatures.getSchema.getGeometryDescriptor.getName)
    val geomFilters = inputFeatures.features().map { sf =>
      val dist: Double = requestedUnit match {
        case "degrees" => sf.geometry.distanceDegrees(bufferDistance)
        case _         => bufferDistance
      }
      ff.dwithin(geomProperty, ff.literal(sf.geometry), dist, "meters")
    }
    ff.or(geomFilters.toSeq)
  }
}

case class ProximityResult(results: SimpleFeatureCollection) extends AbstractCalcResult