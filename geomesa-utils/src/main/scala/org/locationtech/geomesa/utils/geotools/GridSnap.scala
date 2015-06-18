/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.utils.geotools

import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.grid.DefaultGridFeatureBuilder
import org.geotools.grid.oblong.Oblongs
import org.geotools.referencing.crs.DefaultGeographicCRS
import scala.collection.mutable

import scala.math.abs

class GridSnap(env: Envelope, xSize: Int, ySize: Int) {

  val dx = env.getWidth / (xSize - 1)
  val dy = env.getHeight / (ySize - 1)

  /**
   * Computes the X ordinate of the i'th grid column.
   * @param i the index of a grid column
   * @return the X ordinate of the column
   */
  def x(i: Int): Double =
    if (i < 0) {
      env.getMinX
    } else if (i >= xSize - 1) {
      env.getMaxX
    } else {
      env.getMinX + i * dx
    }

  /**
   * Computes the Y ordinate of the i'th grid row.
   * @param j the index of a grid row
   * @return the Y ordinate of the row
   */
  def y(j: Int): Double =
    if (j < 0) {
      env.getMinY
    } else if (j >= ySize - 1) {
      env.getMaxY
    } else {
      env.getMinY + j * dy
    }

  /**
   * Computes the column index of an X ordinate.
   * @param x the X ordinate
   * @return the column index
   */
  def i(x: Double): Int =
    if (x > env.getMaxX) {
      xSize - 1
    } else if (x < env.getMinX) {
      0
    } else {
      val ret = (x - env.getMinX) / dx
      if (ret >= xSize) xSize - 1 else ret.toInt
    }

  /**
   * Computes the column index of an Y ordinate.
   * @param y the Y ordinate
   * @return the column index
   */
  def j(y: Double): Int =
    if (y > env.getMaxY) {
      ySize - 1
    } else if (y < env.getMinY) {
      0
    } else {
      val ret = (y - env.getMinY) / dy
      if (ret >= ySize) ySize - 1 else ret.toInt
    }

  def snap(x: Double, y: Double): (Double, Double) = (this.x(i(x)), this.y(j(y)))
//
//  lazy val precision = {
//    val max = math.max(dx, dy)
//    if (max < 1) {
//      math.pow(10, max.toString.dropWhile(_ != '.').drop(1).indexWhere(_ != '0') + 2).toInt
//    } else {
//      10
//    }
//  }

  /** Generate a Sequence of Coordinates between two given Snap Coordinates using Bresenham's Line Algorithm */
  def snapBresenham(x0: Int, y0: Int, x1: Int, y1: Int): Set[(Int, Int)] = {
//    val (deltaX, deltaY) = (abs(x1 - x0), abs(y1 - y0))
//    if (deltaX == 0 && deltaY == 0) {
//      Set.empty
//    } else {
//      val stepX = if (x0 < x1) 1 else -1
//      val stepY = if (y0 < y1) 1 else -1
//      val fX = stepX * x1
//      val fY = stepY * y1
//      var xT = x0
//      var yT = y0
//      var error = (if (deltaX > deltaY) deltaX else -deltaY) / 2
//      val result = mutable.HashSet.empty[(Int, Int)]
//      while (stepX * xT <= fX && stepY * yT <= fY) {
//        val errorT = error
//        if(errorT > -deltaX){ error -= deltaY; xT += stepX }
//        if(errorT < deltaY){ error += deltaX; yT += stepY }
//        result.add((x(xT), y(yT)))
//      }
//      result
//      iter.toList.dropRight(1).toSet
//    }
    Set.empty
  }

  /** Generate a Set of Coordinates between two given Snap Coordinates which includes both start and end points*/
  def snapLine(start: (Double, Double), end: (Double, Double)): Set[(Int, Int)] = {
    val iStart = i(start._1)
    val jStart = j(start._2)
    val iEnd = i(end._1)
    val jEnd = j(end._2)
    snapBresenham(iStart, jStart, iEnd, jEnd) ++ Set((iStart, iEnd), (jStart, jEnd))
  }

  /** Generate a Sequence of Coordinates between two given Snap Coordinates using Bresenham's Line Algorithm */
  def genBresenhamCoordSet(x0: Int, y0: Int, x1: Int, y1: Int): Set[Coordinate] = {
    val ( deltaX, deltaY ) = (abs(x1 - x0), abs(y1 - y0))
    if ((deltaX == 0) && (deltaY == 0)) Set[Coordinate](new Coordinate(x(x0), y(y0)))
    else {
      val ( stepX, stepY ) = (if (x0 < x1) 1 else -1, if (y0 < y1) 1 else -1)
      val (fX, fY) =  ( stepX * x1, stepY * y1 )
      def iter = new Iterator[Coordinate] {
        var (xT, yT) = (x0, y0)
        var error = (if (deltaX > deltaY) deltaX else -deltaY) / 2
        def next() = {
          val errorT = error
          if(errorT > -deltaX){ error -= deltaY; xT += stepX }
          if(errorT < deltaY){ error += deltaX; yT += stepY }
          new Coordinate(x(xT), y(yT))
        }
        def hasNext = stepX * xT <= fX && stepY * yT <= fY
      }
      iter.toList.dropRight(1).toSet
    }
  }

  /** Generate a Set of Coordinates between two given Snap Coordinates which includes both start and end points*/
  def generateLineCoordSet(coordOne: Coordinate, coordTwo: Coordinate): Set[Coordinate] = {
    genBresenhamCoordSet(i(coordOne.x), j(coordOne.y), i(coordTwo.x), j(coordTwo.y)) + coordOne + coordTwo
  }

  /** return a SimpleFeatureSource grid the same size and extent as the bbox */
  lazy val generateCoverageGrid:SimpleFeatureSource = {
    val dxt = env.getWidth / xSize
    val dyt = env.getHeight / ySize
    val gridBounds = new ReferencedEnvelope(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY, DefaultGeographicCRS.WGS84)
    val gridBuilder = new DefaultGridFeatureBuilder()
    val grid = Oblongs.createGrid(gridBounds, dxt, dyt, gridBuilder)
    grid
  }
}
