/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.tables.AvailableTables
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.filter
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.filter.{decomposeAnd, decomposeOr}
import org.locationtech.geomesa.utils.geotools.SftBuilder.Opts
import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.filter._
import org.opengis.filter.temporal.During
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class QueryFilterSplitterTest extends Specification {

  val sft = new SftBuilder()
    .stringType("attr1")
    .stringType("attr2", index = true)
    .stringType("high", Opts(index = true, cardinality = Cardinality.HIGH))
    .stringType("low", Opts(index = true, cardinality = Cardinality.LOW))
    .date("dtg", default = true)
    .point("geom", default = true)
    .withIndexes(AvailableTables.DefaultTablesStr)
    .build("QueryFilterSplitterTest")

  val ff = CommonFactoryFinder.getFilterFactory2
  val splitter = new QueryFilterSplitter(sft)

  val geom                = "BBOX(geom,40,40,50,50)"
  val geom2               = "BBOX(geom,60,60,70,70)"
  val geomOverlap         = "BBOX(geom,35,35,55,55)"
  val dtg                 = "dtg DURING 2014-01-01T00:00:00Z/2014-01-01T23:59:59Z"
  val dtg2                = "dtg DURING 2014-01-02T00:00:00Z/2014-01-02T23:59:59"
  val dtgOverlap          = "dtg DURING 2014-01-01T00:00:00Z/2014-01-02T23:59:59Z"
  val nonIndexedAttr      = "attr1 = 'test'"
  val nonIndexedAttr2     = "attr1 = 'test2'"
  val indexedAttr         = "attr2 = 'test'"
  val indexedAttr2        = "attr2 = 'test2'"
  val highCardinaltiyAttr = "high = 'test'"
  val lowCardinaltiyAttr  = "low = 'test'"

  val wholeWorld          = "BBOX(geom,-180,-90,180,90)"

  val includeStrategy     = StrategyType.Z3

  def and(clauses: Filter*) = ff.and(clauses)
  def or(clauses: Filter*)  = ff.or(clauses)
  def and(clauses: String*)(implicit d: DummyImplicit) = ff.and(clauses.map(ECQL.toFilter))
  def or(clauses: String*)(implicit d: DummyImplicit)  = ff.or(clauses.map(ECQL.toFilter))
  def not(clauses: String*) = filter.andFilters(clauses.map(ECQL.toFilter).map(ff.not))(ff)
  def f(filter: String)     = ECQL.toFilter(filter)

  def compareAnd(primary: Option[Filter], clauses: Filter*): MatchResult[Option[Seq[Filter]]] =
    primary.map(decomposeAnd) must beSome(containTheSameElementsAs(clauses))
  def compareAnd(primary: Option[Filter], clauses: String*)
                (implicit d: DummyImplicit): MatchResult[Option[Seq[Filter]]] =
    compareAnd(primary, clauses.map(ECQL.toFilter): _*)

  def compareOr(primary: Option[Filter], clauses: Filter*): MatchResult[Option[Seq[Filter]]] =
    primary.map(decomposeOr) must beSome(containTheSameElementsAs(clauses))
  def compareOr(primary: Option[Filter], clauses: String*)
                (implicit d: DummyImplicit): MatchResult[Option[Seq[Filter]]] =
    compareOr(primary, clauses.map(ECQL.toFilter): _*)

  //    TODO    (05:18:55 PM) chris.eichelberger.remote: AND
  //            (05:18:55 PM) chris.eichelberger.remote: +- OR
  //            (05:18:55 PM) chris.eichelberger.remote: |  +- AND
  //            (05:18:55 PM) chris.eichelberger.remote: |  |  +-OR(prop1=FOO, prop1=BAR)
  //            (05:18:55 PM) chris.eichelberger.remote: |  |  +-NOT(prop2 IS NULL)
  //            (05:18:55 PM) chris.eichelberger.remote: |  |  \-OR(prop3=QAZ, prop3=WSX)
  //            (05:18:55 PM) chris.eichelberger.remote: |  +- AND
  //            (05:18:55 PM) chris.eichelberger.remote: |     +-OR(prop1=BAZ, prop1=JIMBO)
  //            (05:18:55 PM) chris.eichelberger.remote: |     +-NOT(prop2 IS NULL)
  //            (05:18:55 PM) chris.eichelberger.remote: |     \-OR(prop3=QAZ, prop3=WSX)
  //            (05:18:55 PM) chris.eichelberger.remote: +- AND
  //            (05:18:55 PM) chris.eichelberger.remote:    +- OR
  //            (05:18:55 PM) chris.eichelberger.remote:       +- BBOX-0
  //            (05:18:55 PM) chris.eichelberger.remote:       \- BBOX-1
  //            "(bbox(geom, -120, 45, -121, 46) OR bbox(geom, -120, 48, -121, 49)) AND "
  //            "((prop1 IN ('foo', 'bar') AND prop2 IS NOT NULL AND prop3 IN ('qaz', 'wsx')) OR "
  //             "(prop1 IN ('baz', 'jim') AND prop2 IS NOT NULL AND prop3 IN ('qaz', 'wsx')))"

  "QueryFilterSplitter" should {

    "return for filter include" >> {
      val filter = Filter.INCLUDE
      val options = splitter.getQueryOptions(Filter.INCLUDE)
      options must haveLength(1)
      options.head.filters must haveLength(1)
      options.head.filters.head.strategy mustEqual StrategyType.Z3
      options.head.filters.head.primary must beNone
      options.head.filters.head.secondary must beNone
    }

    "return none for filter exclude" >> {
      val options = splitter.getQueryOptions(Filter.EXCLUDE)
      options must beEmpty
    }

    "work for spatio-temporal queries" >> {
      "with a simple and" >> {
        val filter = and(geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(f(dtg))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        z3.filters.head.primary.map(decomposeAnd) must beSome(containTheSameElementsAs(Seq(f(geom), f(dtg))))
        z3.filters.head.secondary must beNone
      }

      "with multiple geometries" >> {
        val filter = and(geom, geom2, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        compareAnd(z2.filters.head.primary, geom, geom2)
        z2.filters.head.secondary must beSome(f(dtg))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, geom2, dtg)
        z3.filters.head.secondary must beNone
      }

      "with multiple dates" >> {
        val filter = and(geom, dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(and(dtg, dtgOverlap))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, dtg, dtgOverlap)
        z3.filters.head.secondary must beNone
      }

      "with multiple geometries and dates" >> {
        val filter = and(geom, geomOverlap, dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(and(geom, geomOverlap))
        z2.filters.head.secondary must beSome(and(dtg, dtgOverlap))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, geomOverlap, dtg, dtgOverlap)
        z3.filters.head.secondary must beNone
      }

      "with single attribute ors" >> {
        val filter = and(or(geom, geom2), f(dtg))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must containTheSameElementsAs(Seq(StrategyType.Z2, StrategyType.Z3))
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        compareOr(z2.filters.head.primary, geom, geom2)
        forall(z2.filters.map(_.secondary))(_ must beSome)
        z2.filters.map(_.secondary.get) must contain(beAnInstanceOf[During], beAnInstanceOf[And])
        z2.filters.map(_.secondary.get).collect { case a: And => a.getChildren }.flatten must
            contain(beAnInstanceOf[During], beAnInstanceOf[Not])
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, or(geom, geom2), f(dtg))
        z3.filters.head.secondary must beNone
      }

      "with multiple attribute ors" >> {
        val filter = or(and(geom, dtg), and(geom2, dtg2))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(4)
        forall(options)(_.filters must haveLength(2))
        options.map(_.filters.map(_.strategy).toSet) must
            containTheSameElementsAs(Seq(Set(StrategyType.Z2), Set(StrategyType.Z3),
              Set(StrategyType.Z2, StrategyType.Z3), Set(StrategyType.Z2, StrategyType.Z3)))
        val z2 = options.find(_.filters.forall(_.strategy == StrategyType.Z2)).get
        z2.filters.map(_.primary) must contain(beSome(f(geom)), beSome(f(geom2)))
        forall(z2.filters.map(_.secondary))(_ must beSome)
        z2.filters.map(_.secondary.get) must contain(beAnInstanceOf[During], beAnInstanceOf[And])
        z2.filters.map(_.secondary.get).collect { case a: And => a.getChildren }.flatten must
            contain(beAnInstanceOf[During], beAnInstanceOf[Not])
        val z3 = options.find(_.filters.forall(_.strategy == StrategyType.Z3)).get
        z3.filters.map(_.primary) must
            contain(compareAnd(_: Option[Filter], geom, dtg), compareAnd(_: Option[Filter], geom2, dtg2))
        z3.filters.map(_.secondary).filter(_.isDefined) must haveLength(1)
        z3.filters.map(_.secondary).filter(_.isDefined).head.get must beAnInstanceOf[Not]
      }

      "with spatiotemporal and non-indexed attributes clauses" >> {
        val filter = and(geom, dtg, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(and(dtg, nonIndexedAttr))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, dtg)
        z3.filters.head.secondary must beSome(f(nonIndexedAttr))
      }

      "with spatiotemporal and indexed attributes clauses" >> {
        val filter = and(geom, dtg, indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(3)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3, StrategyType.ATTRIBUTE)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(and(dtg, indexedAttr))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, dtg)
        z3.filters.head.secondary must beSome(f(indexedAttr))
        val attr = options.find(_.filters.head.strategy == StrategyType.ATTRIBUTE).get
        attr.filters.head.primary must beSome(f(indexedAttr))
        attr.filters.head.secondary must beSome(and(geom, dtg))
      }

      "with spatiotemporal, indexed and non-indexed attributes clauses" >> {
        val filter = and(geom, dtg, indexedAttr, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(3)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3, StrategyType.ATTRIBUTE)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(and(dtg, indexedAttr, nonIndexedAttr))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, dtg)
        z3.filters.head.secondary must beSome(and(indexedAttr, nonIndexedAttr))
        val attr = options.find(_.filters.head.strategy == StrategyType.ATTRIBUTE).get
        attr.filters.head.primary must beSome(f(indexedAttr))
        attr.filters.head.secondary must beSome(and(geom, dtg, nonIndexedAttr))
      }

      "with spatiotemporal clauses and non-indexed attributes or" >> {
        val filter = and(f(geom), f(dtg), or(nonIndexedAttr, nonIndexedAttr2))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(and(f(dtg), or(nonIndexedAttr, nonIndexedAttr2)))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, dtg)
        z3.filters.head.secondary must beSome(or(nonIndexedAttr, nonIndexedAttr2))
      }

      "while ignoring world-covering geoms" >> {
        val filter = QueryPlanFilterVisitor(null, f(wholeWorld))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual includeStrategy
        options.head.filters.head.primary must beNone
        options.head.filters.head.secondary must beNone
      }

      "while ignoring world-covering geoms when other filters are present" >> {
        val filter = QueryPlanFilterVisitor(null, and(wholeWorld, geom, dtg))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(f(dtg))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, dtg)
        z3.filters.head.secondary must beNone
      }
    }

    "work for single clause filters" >> {
      "spatial" >> {
        val filter = f(geom)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z2
        options.head.filters.head.primary must beSome(filter)
        options.head.filters.head.secondary must beNone
      }

      "temporal" >> {
        val filter = f(dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        options.head.filters.head.primary must beSome(filter)
        options.head.filters.head.secondary must beNone
      }

      "non-indexed attributes" >> {
        val filter = f(nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual includeStrategy
        options.head.filters.head.primary must beNone
        options.head.filters.head.secondary must beSome(filter)
      }

      "indexed attributes" >> {
        val filter = f(indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary must beSome(filter)
        options.head.filters.head.secondary must beNone
      }

      "low-cardinality attributes" >> {
        val filter = f(lowCardinaltiyAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary must beSome(filter)
        options.head.filters.head.secondary must beNone
      }

      "high-cardinality attributes" >> {
        val filter = f(highCardinaltiyAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary must beSome(filter)
        options.head.filters.head.secondary must beNone
      }
    }

    "work for simple ands" >> {
      "spatial" >> {
        val filter = and(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z2
        compareAnd(options.head.filters.head.primary, geom, geom2)
        options.head.filters.head.secondary must beNone
      }

      "temporal" >> {
        val filter = and(dtg, dtgOverlap)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z3
        compareAnd(options.head.filters.head.primary, dtg, dtgOverlap)
        options.head.filters.head.secondary must beNone
      }

      "non-indexed attributes" >> {
        val filter = and(nonIndexedAttr, nonIndexedAttr2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual includeStrategy
        options.head.filters.head.primary must beNone
        options.head.filters.head.secondary must beSome(filter)
      }

      "indexed attributes" >> {
        val filter = and(indexedAttr, indexedAttr2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary must beSome(filter)
        options.head.filters.head.secondary must beNone
      }

      "low-cardinality attributes" >> {
        val filter = and(lowCardinaltiyAttr, nonIndexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
        options.head.filters.head.primary must beSome(f(lowCardinaltiyAttr))
        options.head.filters.head.secondary must beSome(f(nonIndexedAttr))
      }
    }

    "split filters on OR" >> {
      "with spatiotemporal clauses" >> {
        val filter = or(geom, dtg)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(2)
        val z3 = options.head.filters.find(_.strategy == StrategyType.Z3)
        z3 must beSome
        z3.get.primary must beSome(f(dtg))
        z3.get.secondary must beSome(not(geom))
        val st = options.head.filters.find(_.strategy == StrategyType.Z2)
        st must beSome
        st.get.primary must beSome(f(geom))
        st.get.secondary must beNone
      }

      "with multiple spatial clauses" >> {
        val filter = or(geom, geom2)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(1)
        options.head.filters.head.strategy mustEqual StrategyType.Z2
        compareOr(options.head.filters.head.primary, geom, geom2)
        options.head.filters.head.secondary must beNone
      }

      "with spatiotemporal and indexed attribute clauses" >> {
        val filter = or(geom, indexedAttr)
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(2)
        options.head.filters.map(_.strategy) must containTheSameElementsAs(Seq(StrategyType.Z2, StrategyType.ATTRIBUTE))
        options.head.filters.find(_.strategy == StrategyType.Z2).get.primary must beSome(f(geom))
        options.head.filters.find(_.strategy == StrategyType.Z2).get.secondary must beNone
        options.head.filters.find(_.strategy == StrategyType.ATTRIBUTE).get.primary must beSome(f(indexedAttr))
        options.head.filters.find(_.strategy == StrategyType.ATTRIBUTE).get.secondary must beSome(not(geom))
      }

      "and collapse overlapping query filters" >> {
        "with spatiotemporal and non-indexed attribute clauses" >> {
          val filter = or(geom, nonIndexedAttr)
          val options = splitter.getQueryOptions(filter)
          options must haveLength(1)
          options.head.filters must haveLength(1)
          options.head.filters.head.strategy mustEqual includeStrategy
          options.head.filters.head.primary must beNone
          options.head.filters.head.secondary must beSome(filter)
        }
      }
    }

    "split nested filters" >> {
      "with ANDs" >> {
        val filter = and(f(geom), and(dtg, nonIndexedAttr))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(2)
        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must contain(StrategyType.Z2, StrategyType.Z3)
        val z2 = options.find(_.filters.head.strategy == StrategyType.Z2).get
        z2.filters.head.primary must beSome(f(geom))
        z2.filters.head.secondary must beSome(and(dtg, nonIndexedAttr))
        val z3 = options.find(_.filters.head.strategy == StrategyType.Z3).get
        compareAnd(z3.filters.head.primary, geom, dtg)
        z3.filters.head.secondary must beSome(f(nonIndexedAttr))
      }

      "with ORs" >> {
        val filter = or(f(geom), or(dtg, indexedAttr))
        val options = splitter.getQueryOptions(filter)
        options must haveLength(1)
        options.head.filters must haveLength(3)
        options.head.filters.map(_.strategy) must
            containTheSameElementsAs(Seq(StrategyType.Z3, StrategyType.Z2, StrategyType.ATTRIBUTE))
        options.head.filters.map(_.primary) must contain(beSome(f(geom)), beSome(f(dtg)), beSome(f(indexedAttr)))
        options.head.filters.map(_.secondary) must contain(beSome(not(geom)), beSome(not(geom, dtg)))
        options.head.filters.map(_.secondary) must contain(beNone)
      }
    }

    "support indexed date attributes" >> {
      val sft = SimpleFeatureTypes.createType("dtgIndex", "dtg:Date:index=full,*geom:Point:srid=4326")
      val splitter = new QueryFilterSplitter(sft)
      val filter = f("dtg TEQUALS 2014-01-01T12:30:00.000Z")
      val options = splitter.getQueryOptions(filter)
      options must haveLength(1)
      options.head.filters must haveLength(1)
      options.head.filters.head.strategy mustEqual StrategyType.ATTRIBUTE
      options.head.filters.head.primary must beSome(filter)
      options.head.filters.head.secondary must beNone
    }

    "provide only one option on OR queries of high cardinality indexed attributes" >> {
      def testHighCard(attrPart: String): MatchResult[Any] = {
        val bbox = "BBOX(geom, 40.0,40.0,50.0,50.0)"
        val dtg  = "dtg DURING 2014-01-01T00:00:00+00:00/2014-01-01T23:59:59+00:00"
        val filter = f(s"($attrPart) AND $bbox AND $dtg")
        val options = splitter.getQueryOptions(filter)
        options must haveLength(3)

        forall(options)(_.filters must haveLength(1))
        options.map(_.filters.head.strategy) must
            containTheSameElementsAs(Seq(StrategyType.ATTRIBUTE, StrategyType.Z2, StrategyType.Z3))

        val attrQueryFilter = options.find(_.filters.head.strategy == StrategyType.ATTRIBUTE).get.filters.head
        compareOr(attrQueryFilter.primary, decomposeOr(f(attrPart)): _*)
        compareAnd(attrQueryFilter.secondary, bbox, dtg)

        val z2QueryFilters = options.find(_.filters.head.strategy == StrategyType.Z2).get.filters.head
        z2QueryFilters.primary must beSome(f(bbox))
        z2QueryFilters.secondary must beSome(beAnInstanceOf[And])
        val z2secondary = z2QueryFilters.secondary.get.asInstanceOf[And].getChildren.toSeq
        z2secondary must haveLength(2)
        z2secondary must contain(beAnInstanceOf[Or], beAnInstanceOf[During])
        compareOr(z2secondary.find(_.isInstanceOf[Or]), decomposeOr(f(attrPart)): _*)
        z2secondary.find(_.isInstanceOf[During]).get mustEqual f(dtg)

        val z3QueryFilters = options.find(_.filters.head.strategy == StrategyType.Z3).get.filters.head
        compareAnd(z3QueryFilters.primary, bbox, dtg)
        compareOr(z3QueryFilters.secondary, decomposeOr(f(attrPart)): _*)
      }

      val orQuery = (0 until 5).map( i => s"high = 'h$i'").mkString(" OR ")
      val inQuery = s"high in (${(0 until 5).map( i => s"'h$i'").mkString(",")})"
      Seq(orQuery, inQuery).forall(testHighCard)
    }
  }
}
