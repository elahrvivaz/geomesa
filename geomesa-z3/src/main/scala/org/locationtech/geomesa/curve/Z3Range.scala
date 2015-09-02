/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve


object Z3Range {


  /**
   * Recurse down the oct-tree and report all z-ranges which are contained
   * in the cube defined by the min and max points
   */
  def zranges(min: Z3, max: Z3): Seq[(Long, Long)] = {
    val (commonPrefix, commonBits) = longestCommonPrefix3(min.z, max.z)

    // base our recursion on the depth of the tree that we get 'for free' from the common prefix
    val maxRecurse = if (commonBits < 30) 7 else if (commonBits < 40) 6 else 5

    val searchRange = Z3Range(min, max)
    var mq = new MergeQueue // stores our results

    def zranges(prefix: Long, offset: Int, oct: Long, level: Int): Unit = {
      val min: Long = prefix | (oct << offset) // QR + 000...
      val max: Long = min | (1L << offset) - 1 // QR + 111...
      val octRange = Z3Range(new Z3(min), new Z3(max))

      if (searchRange containsInUserSpace octRange) {
        // whole range matches, happy day
        mq += (octRange.min.z, octRange.max.z)
      } else if (searchRange overlapsInUserSpace octRange) {
        if (level < maxRecurse && offset > 0) {
          // some portion of this range is excluded
          // let our children work on each subrange
          val nextOffset = offset - 3
          val nextLevel = level + 1
          zranges(min, nextOffset, 0, nextLevel)
          zranges(min, nextOffset, 1, nextLevel)
          zranges(min, nextOffset, 2, nextLevel)
          zranges(min, nextOffset, 3, nextLevel)
          zranges(min, nextOffset, 4, nextLevel)
          zranges(min, nextOffset, 5, nextLevel)
          zranges(min, nextOffset, 6, nextLevel)
          zranges(min, nextOffset, 7, nextLevel)
        } else {
          // bottom out - add the entire range so we don't miss anything
          mq += (octRange.min.z, octRange.max.z)
        }
      }
    }

    // kick off recursion over the narrowed space
    zranges(commonPrefix, 62 - commonBits, 0, 0)

    // return our aggregated results
    mq.toSeq
  }

  /**
   * Calculates the longest common binary prefix between two z longs
   *
   * @return (common prefix, number of bits in common)
   */
  def longestCommonPrefix3(lower: Long, upper: Long): (Long, Int) = {
    var bitShift = 62 - 3
    while ((lower >>> bitShift) == (upper >>> bitShift) && bitShift > -1) {
      bitShift -= 3
    }
    bitShift += 3 // increment back to the last valid value
    (lower & (Long.MaxValue << bitShift), 62 - bitShift)
  }

  /**
   * Represents a cube in index space defined by min and max as two opposing points.
   * All operations refer to index space.
   */
  case class Z3Range(min: Z3, max: Z3) {

    require(min.z <= max.z, s"Not: $min < $max")

    def mid: Z3 = Z3((max.z - min.z) / 2)

    def length: Int = (max.z - min.z + 1).toInt

    // contains in index space (e.g. the long value)
    def contains(bits: Z3): Boolean = bits.z >= min.z && bits.z <= max.z

    // contains in index space (e.g. the long value)
    def contains(r: Z3Range): Boolean = contains(r.min) && contains(r.max)

    // overlap in index space (e.g. the long value)
    def overlaps(r: Z3Range): Boolean = contains(r.min) || contains(r.max)

    // contains in user space - each dimension is contained
    def containsInUserSpace(bits: Z3) = {
      val (x, y, z) = bits.decode
      x >= min.dim(0) && x <= max.dim(0) && y >= min.dim(1) && y <= max.dim(1) && z >= min.dim(2) && z <= max.dim(2)
    }

    // contains in user space - each dimension is contained
    def containsInUserSpace(r: Z3Range): Boolean = containsInUserSpace(r.min) && containsInUserSpace(r.max)

    // overlap in user space - if any dimension overlaps
    def overlapsInUserSpace(r: Z3Range): Boolean =
      overlaps(min.dim(0), max.dim(0), r.min.dim(0), r.max.dim(0)) &&
          overlaps(min.dim(1), max.dim(1), r.min.dim(1), r.max.dim(1)) &&
          overlaps(min.dim(2), max.dim(2), r.min.dim(2), r.max.dim(2))

    private def overlaps(a1: Int, a2: Int, b1: Int, b2: Int) = math.max(a1, b1) <= math.min(a2, b2)

  }

}
