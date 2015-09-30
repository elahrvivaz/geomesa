/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */
package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.ZRange.ZPrefix

class Z2(val z: Long) extends AnyVal with ZPoint {
  override def dims = Z2.dims
  override def dim(i: Int) = if (i == 0) Z2.combine(z) else Z2.combine(z >> i)
  override def decode = (dim(0), dim(1))
  override def toString = f"$z $decode"
}

object Z2 extends ZN {

  override final val dims = 2
  override final val bits = 60
  override final val bitsPerDim = 30
  override final val maxValue = 0x3fffffffL

  override def apply(z: Long) = new Z2(z)
  override def apply(dims: Int*) = new Z2(split(dims.head) | split(dims(1)) << 1)

  /** insert 0 between every bit in value. Only first 30 bits can be considered. */
  def split(value: Long): Long = {
    var x = value & maxValue
    x = (x | x << 16) & 0x00003fff0000ffffL
    x = (x | x << 8)  & 0x003f00ff00ff00ffL
    x = (x | x << 4)  & 0x030f0f0f0f0f0f0fL
    x = (x | x << 2)  & 0x0333333333333333L
    (x | x << 1)      & 0x0555555555555555L
  }

  /** combine every second bit to form a value. Maximum value is 30 bits. */
  def combine(z: Long): Int = {
    var x = z & 0x0555555555555555L
    x = (x ^ (x >>  1)) & 0x0333333333333333L
    x = (x ^ (x >>  2)) & 0x030f0f0f0f0f0f0fL
    x = (x ^ (x >>  4)) & 0x003f00ff00ff00ffL
    x = (x ^ (x >>  8)) & 0x00003fff0000ffffL
    x = (x ^ (x >> 16)) & maxValue
    x.toInt
  }
//
//  def decompose(geom: Geometry, precision: Long = 16): Seq[ZPrefix] = {
//    geom match {
//      case g: Point              => decompose(g, precision)
//      case g: LineString         => decompose(g, precision)
//      case g: Polygon            => decompose(g, precision)
//      case g: GeometryCollection => decompose(g, precision)
//      case _ => throw new IllegalArgumentException(s"Decompose for ${geom.getClass.getCanonicalName} not supported")
//    }
//  }
//
//  def decompose(g: Point, precision: Long): Seq[ZPrefix] = Seq(ZPrefix(Z2SFC.index(g.getX, g.getY).z, 64))
//
//  def decompose(g: LineString, precision: Long): Seq[ZPrefix] = {
////    val zs = (0 until g.getNumPoints).map(g.getCoordinateN).sliding(2)
////    zs.toSeq.flatMap { case Seq(c1, c2) =>
////      val ls = gf.createLineString(Array(c1, c2))
////      val env = ls.getEnvelopeInternal
////      val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
////      decompose(Z2SFC.index(lx, ly), Z2SFC.index(ux, uy), ls, precision)
////    }
//    val env = g.getEnvelopeInternal
//    val ll = Z2SFC.index(env.getMinX, env.getMinY)
//    val ur = Z2SFC.index(env.getMaxX, env.getMaxY)
//    decompose(ll, ur, g, precision)
//  }
//
//  def decompose(g: Polygon, precision: Long): Seq[ZPrefix] = {
//    val env = g.getEnvelopeInternal
//    val ll = Z2SFC.index(env.getMinX, env.getMinY)
//    val ur = Z2SFC.index(env.getMaxX, env.getMaxY)
//    decompose(ll, ur, g, precision)
//  }
//
//  val gf = new GeometryFactory()
//
//  private def decompose(ll: Z2, ur: Z2, g: Geometry, precision: Long): Seq[ZPrefix] = {
//    val candidates = scala.collection.mutable.Queue((ll, ur))
//    val results = ArrayBuffer.empty[ZPrefix]
//    while (candidates.nonEmpty) {
//      val (ll, ur) = candidates.dequeue()
//      val prefix = ZRange.longestCommonPrefix(ll.z, ur.z, Z2)
//      if (prefix.precision >= precision) {
//        results.append(prefix)
//      } else {
////        println(s"precision ${prefix.precision}")
//        val zn = Z2((ll.z + ur.z) / 2)
//
////        val (lx, ly) = Z2SFC.invert(ll)
////        val (ux, uy) = Z2SFC.invert(ur)
////        val mx = (lx + ux) / 2
////        val my = (ly + uy) / 2
////        val children = Seq((lx, ly, mx, my), (mx, my, ux, uy), (mx, ly, ux, my), (lx, my, mx, uy))
////        val intersecting = children.filter { case (minx, miny, maxx, maxy) =>
////          val coords = Array[Coordinate](
////            new Coordinate(minx, miny),
////            new Coordinate(minx, maxy),
////            new Coordinate(maxx, maxy),
////            new Coordinate(maxx, miny),
////            new Coordinate(minx, miny)
////          )
////          val poly = gf.createPolygon(gf.createLinearRing(coords), null)
////          val res = g.intersects(poly)
////          println(s"$res comparing ${poly.getEnvelopeInternal} to $g")
////          res
////        }
////        println(s"adding ${intersecting.length} candidates")
////        intersecting.foreach { case (minx, miny, maxx, maxy) =>
////          candidates.enqueue((Z2SFC.index(minx, miny), Z2SFC.index(maxx, maxy)))
////        }
//        candidates.enqueue((ll, zn), (zn, ur))
//      }
////      println(s"results ${results.length} candidates: ${candidates.length}")
//    }
//    results.toSeq
//  }
//
//// 47.30967787376657 22.40536415671486, 45.50437005117634 25.975452777972016
////  false comparing POLYGON ((47.10937521151209  22.80147335752983, 47.10937521151209 22.80147344134886, 47.109375043874024 22.80147344134886, 47.109375043874024 22.80147335752983, 47.10937521151209 22.80147335752983)) to LINESTRING (47.30967787376657 22.40536415671486, 45.50437005117634 25.975452777972016)
////  true  comparing POLYGON ((47.109375043874024 22.80147344134886, 47.109375043874024 22.801473525167893, 47.10937487623596 22.801473525167893, 47.10937487623596 22.80147344134886, 47.109375043874024 22.80147344134886)) to LINESTRING (47.30967787376657 22.40536415671486, 45.50437005117634 25.975452777972016)
////  true  comparing POLYGON ((47.109375043874024 22.80147335752983, 47.109375043874024 22.80147344134886, 47.10937487623596 22.80147344134886, 47.10937487623596 22.80147335752983, 47.109375043874024 22.80147335752983)) to LINESTRING (47.30967787376657 22.40536415671486, 45.50437005117634 25.975452777972016)
////  false comparing POLYGON ((47.10937521151209  22.80147344134886, 47.10937521151209 22.801473525167893, 47.109375043874024 22.801473525167893, 47.109375043874024 22.80147344134886, 47.10937521151209 22.80147344134886))
//  def decompose(g: GeometryCollection, precision: Long): Seq[ZPrefix] =
//    (0 until g.getNumGeometries).map(g.getGeometryN).flatMap(decompose(_, precision))
//
//  def zBox(geom: Geometry): ZPrefix = {
//    val env = geom.getEnvelopeInternal
//    zBox(Z2SFC.index(env.getMinX, env.getMinY), Z2SFC.index(env.getMaxX, env.getMaxY))
//  }

  def zBox(ll: Z2, ur: Z2): ZPrefix = ZRange.longestCommonPrefix(ll.z, ur.z, Z2)
}
