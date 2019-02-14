/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JakesTest extends Specification with TestWithDataStore {

  sequential

  override val spec =
  """
    |mmsi:Long,
    |imo:Integer,
    |vessel_name:String,
    |callsign:String,
    |vessel_type:String,
    |vessel_type_code:Integer,
    |vessel_type_cargo:String,
    |vessel_class:String,
    |length:Integer,
    |width:Integer,
    |flag_country:String,
    |flag_code:Integer,
    |destination:String,
    |eta:String,
    |draught:Double,
    |position:Point,
    |longitude:Double,
    |latitude:Double,
    |sog:Double,
    |cog:Double,
    |rot:Double,
    |heading:Double,
    |nav_status:String,
    |nav_status_code:Integer,
    |regionid:Integer,
    |source:String,
    |ts_pos_utc:String,
    |ts_static_utc:String,
    |ts_insert_utc:String,
    |dt_pos_utc:String,
    |dt_static_utc:String,
    |dt_insert_utc:String,
    |vessel_type_main:String,
    |vessel_type_sub:String,
    |message_type:Integer,
    |eeid:Long,
    |dtg:Date
  """.stripMargin

  "AccumuloDataStore" should {
    "foo" in {
      val filter =
        "((BBOX(position,-118.40429742431641,33.66651480102539,-118.4022974243164,33.66851480102539)) AND " +
            "(dtg >= '2018-11-11T14:41:43.000Z' AND dtg <= '2018-11-11T14:41:43.000Z') AND " +
            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.7 AND cog=297 AND heading=296)) " +
//            "OR " +
//            "((BBOX(position,-118.41151483154297,33.669600891113284,-118.40951483154296,33.67160089111328)) AND " +
//            "(dtg >= '2018-11-11T14:43:59.000Z' AND dtg <= '2018-11-11T14:43:59.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.7 AND cog=297 AND heading=298)) " +
//            "OR " +
//            "((BBOX(position,-118.41151483154297,33.669600891113284,-118.40951483154296,33.67160089111328)) AND " +
//            "(dtg >= '2018-11-11T14:44:08.000Z' AND dtg <= '2018-11-11T14:44:08.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.7 AND cog=297 AND heading=298)) " +
//            "OR " +
//            "((BBOX(position,-118.41278131103516,33.670165466308596,-118.41078131103515,33.67216546630859)) AND " +
//            "(dtg >= '2018-11-11T14:44:23.000Z' AND dtg <= '2018-11-11T14:44:23.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.7 AND cog=298 AND heading=299)) " +
//            "OR " +
//            "((BBOX(position,-118.41909844970704,33.673148559570315,-118.41709844970703,33.67514855957031)) AND " +
//            "(dtg >= '2018-11-11T14:46:23.000Z' AND dtg <= '2018-11-11T14:46:23.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.8 AND cog=300 AND heading=302)) " +
//            "OR " +
//            "((BBOX(position,-118.42211968994141,33.674682067871096,-118.4201196899414,33.67668206787109)) AND " +
//            "(dtg >= '2018-11-11T14:47:23.000Z' AND dtg <= '2018-11-11T14:47:23.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.8 AND cog=301 AND heading=301)) " +
//            "OR " +
//            "((BBOX(position,-118.42251641845704,33.67489950561524,-118.42051641845703,33.67689950561523)) AND " +
//            "(dtg >= '2018-11-11T14:47:32.000Z' AND dtg <= '2018-11-11T14:47:32.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.8 AND cog=301 AND heading=301)) " +
//            "OR " +
//            "((BBOX(position,-118.42354638671875,33.675433563232424,-118.42154638671875,33.67743356323242)) AND " +
//            "(dtg >= '2018-11-11T14:47:53.000Z' AND dtg <= '2018-11-11T14:47:53.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.8 AND cog=301 AND heading=301)) " +
//            "OR " +
//            "((BBOX(position,-118.42354638671875,33.675433563232424,-118.42154638671875,33.67743356323242)) AND " +
//            "(dtg >= '2018-11-11T14:47:54.000Z' AND dtg <= '2018-11-11T14:47:54.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.8 AND cog=301 AND heading=301)) " +
//            "OR " +
//            "((BBOX(position,-118.42661340332032,33.67698233032227,-118.42461340332031,33.67898233032226)) AND " +
//            "(dtg >= '2018-11-11T14:48:53.000Z' AND dtg <= '2018-11-11T14:48:53.000Z') AND " +
//            "(mmsi=235069077 AND vessel_name='HYUNDAI SPLENDOR' AND callsign='2BSX3' AND length=340 AND width=46 AND sog=10.7 AND cog=300 AND heading=300))"
      ""
      val query = new Query(sftName, ECQL.toFilter(filter))
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results must beEmpty
    }
  }
}
