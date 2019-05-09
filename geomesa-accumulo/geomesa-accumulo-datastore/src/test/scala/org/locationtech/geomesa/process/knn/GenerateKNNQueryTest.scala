/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.knn

import org.geotools.data.Query
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypes, WholeWorldPolygon}
import org.locationtech.jts.geom.GeometryCollection
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GenerateKNNQueryTest extends Specification {

  import org.locationtech.geomesa.filter.ff

  val sft = SimpleFeatureTypes.createType(getClass.getSimpleName, "geom:Point:srid=4326")

  val smallGH = GeoHash("dqb0tg")

  "GenerateKNNQuery" should {
    " inject a small BBOX into a larger query and confirm that the original is untouched" in {
      val q =
        ff.and(
          ff.like(ff.property("prop"), "foo"),
          ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(DefaultGeographicCRS.WGS84))
        )

      // use the above to generate a Query
      val oldQuery = new Query("", q)

      // and then generate a new one
      val newQuery = KNNQuery.generateKNNQuery(smallGH, oldQuery, "geom")

      // check that oldQuery is untouched
      val oldFilter = oldQuery.getFilter

      // confirm that the oldFilter was not mutated by operations on the new filter
      // this confirms that the deep copy on the oldQuery was done properly
      oldFilter mustEqual q
    }

    "inject a small BBOX into a larger query and have the spatial predicate be equal to the GeoHash boundary" in {
      //define a loose BBOX
      val q =
        ff.and(
          ff.like(ff.property("prop"), "foo"),
          ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(DefaultGeographicCRS.WGS84))
        )

      // use the above to generate a Query
      val oldQuery = new Query("", q)

      // and then generate a new one
      val newQuery = KNNQuery.generateKNNQuery(smallGH, oldQuery, "geom")

      // get the newFilter
      val newFilter =  newQuery.getFilter

      // process the newFilter to split out the geometry part
      val (geomFilters, _) = partitionPrimarySpatials(newFilter, sft)

      // rewrite the geometry filter
      val tweakedGeomFilters = geomFilters.map { filter =>
        filter.accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter]
      }

      val geomsToCover = {
        import scala.collection.JavaConversions._
        val geoms = FilterHelper.extractGeometries(ff.and(tweakedGeomFilters), "geom", intersect = true)
        if (geoms.values.length < 2) {
          geoms.values.headOption.orNull
        } else {
          new GeometryCollection(geoms.values.toArray, geoms.values.head.getFactory)
        }
      }

      val geometryToCover = Option(geomsToCover).map(_.intersection(WholeWorldPolygon)).orNull

      // confirm that the extracted spatial predicate matches the GeoHash BBOX.
      geometryToCover.equals(smallGH.geom) must beTrue
    }
  }
}
