/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.sfcurve.IndexRange

abstract class SpaceFillingExtentsCurve3D(xBounds: (Double, Double), yBounds: (Double, Double), zBounds: (Double, Double)) {

  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2
  private val zLo = zBounds._1
  private val zHi = zBounds._2

  private val xSize = xHi - xLo
  private val ySize = yHi - yLo
  private val zSize = zHi - zLo

  /**
    * Index a polygon by it's bounding box
    *
    * @param bounds (xmin, ymin, zmin, xmax, ymax, zmax)
    * @return indexed value for the bounding box
    */
  def index(bounds: (Double, Double, Double, Double, Double, Double)): Long =
    index(bounds._1, bounds._2, bounds._3, bounds._4, bounds._5, bounds._6)

  /**
    * Index a polygon by it's bounding box
    *
    * @param xmin min x value in xBounds
    * @param ymin min y value in yBounds
    * @param zmin min z value in zBounds
    * @param xmax max x value in xBounds, must be >= xmin
    * @param ymax max y value in yBounds, must be >= ymin
    * @param zmax max z value in zBounds, must be >= tmin
    * @return indexed value for the bounding box
    */
  def index(xmin: Double, ymin: Double, zmin: Double, xmax: Double, ymax: Double, zmax: Double): Long

  /**
    * Determine indexed curve ranges that will cover a given query window
    *
    * @param query a window to cover in the form (xmin, ymin, zmin, xmax, ymax, zmax) where all values are in user space
    * @return
    */
  def ranges(query: (Double, Double, Double, Double, Double, Double)): Seq[IndexRange] = ranges(Seq(query))

  /**
    * Determine indexed curve ranges that will cover a given query window
    *
    * @param query a window to cover in the form (xmin, ymin, zmin, xmax, ymax, zmax) where all values are in user space
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(query: (Double, Double, Double, Double, Double, Double), maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq(query), maxRanges)

  /**
    * Determine indexed curve ranges that will cover a given query window
    *
    * @param xmin min x value in user space
    * @param ymin min y value in user space
    * @param zmin min z value in user space
    * @param xmax max x value in user space, must be >= xmin
    * @param ymax max y value in user space, must be >= ymin
    * @param zmax max z value in user space, must be >= zmin
    * @return
    */
  def ranges(xmin: Double, ymin: Double, zmin: Double, xmax: Double, ymax: Double, zmax: Double): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, zmin, xmax, ymax, zmax)))

  /**
    * Determine indexed curve ranges that will cover a given query window
    *
    * @param xmin min x value in user space
    * @param ymin min y value in user space
    * @param zmin min z value in user space
    * @param xmax max x value in user space, must be >= xmin
    * @param ymax max y value in user space, must be >= ymin
    * @param zmax max z value in user space, must be >= zmin
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(xmin: Double,
             ymin: Double,
             zmin: Double,
             xmax: Double,
             ymax: Double,
             zmax: Double,
             maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, zmin, xmax, ymax, zmax)), maxRanges)

  /**
    * Determine indexed curve ranges that will cover a given query window
    *
    * @param queries a sequence of OR'd windows to cover. Each window is in the form
    *                (xmin, ymin, zmin, xmax, ymax, zmax) where all values are in user space
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(queries: Seq[(Double, Double, Double, Double, Double, Double)],
             maxRanges: Option[Int] = None): Seq[IndexRange]


  /**
    * Normalize user space values to [0,1]
    *
    * @param xmin min x value in user space
    * @param ymin min y value in user space
    * @param zmin min z value in user space
    * @param xmax max x value in user space, must be >= xmin
    * @param ymax max y value in user space, must be >= ymin
    * @param zmax max z value in user space, must be >= zmin
    * @return
    */
  protected def normalize(xmin: Double,
                          ymin: Double,
                          zmin: Double,
                          xmax: Double,
                          ymax: Double,
                          zmax: Double): (Double, Double, Double, Double, Double, Double) = {
    require(xmin <= xmax && ymin <= ymax && zmin <= zmax,
      s"Bounds must be ordered: [$xmin $xmax] [$ymin $ymax] [$zmin $zmax]")
    require(xmin >= xLo && xmax <= xHi && ymin >= yLo && ymax <= yHi && zmin >= zLo && zmax <= zHi,
      s"Values out of bounds ([$xLo $xHi] [$yLo $yHi] [$zLo $zHi]): [$xmin $xmax] [$ymin $ymax] [$zmin $zmax]")

    val nxmin = (xmin - xLo) / xSize
    val nymin = (ymin - yLo) / ySize
    val nzmin = (zmin - zLo) / zSize
    val nxmax = (xmax - xLo) / xSize
    val nymax = (ymax - yLo) / ySize
    val nzmax = (zmax - zLo) / zSize

    (nxmin, nymin, nzmin, nxmax, nymax, nzmax)
  }
}
