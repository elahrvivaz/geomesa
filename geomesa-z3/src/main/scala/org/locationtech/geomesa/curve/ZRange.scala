/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */
package org.locationtech.geomesa.curve

/**
 * Represents a cube in index space defined by min and max as two opposing points.
 * All operations refer to user space.
 */
case class ZRange(min: ZPoint, max: ZPoint) {

  // contains in user space - each dimension is contained
  def contains(r: ZRange): Boolean = {
    var i = 0
    while (i < min.dims) {
      if (min.dim(i) > r.min.dim(i) || max.dim(i) < r.max.dim(i)) {
        return false
      }
      i += 1
    }
    true
  }

  // overlap in user space - if all dimensions overlaps
  def overlaps(r: ZRange): Boolean = {
    var i = 0
    while (i < min.dims) {
      if (math.max(min.dim(i), r.min.dim(i)) > math.min(max.dim(i), r.max.dim(i))) {
        return false
      }
      i += 1
    }
    true
  }
}

object ZRange {

  /**
   * Recurse down the oct-tree and report all z-ranges which are contained
   * in the cube defined by the min and max points
   */
  def zranges(min: ZPoint, max: ZPoint, n: ZN, precision: Int = 64): Seq[(Long, Long)] = {
    val ZPrefix(commonPrefix, commonBits) = longestCommonPrefix(min.z, max.z, n)

    val searchRange = ZRange(min, max)
    var mq = new MergeQueue // stores our results

    // base our recursion on the depth of the tree that we get 'for free' from the common prefix,
    // and on the expansion factor of the number child regions, which is proportional to the number of dimensions
    val maxRecurse = (if (commonBits < 31) 21 else if (commonBits < 41) 18 else 15) / n.dims
// TODO consider if z2 should recurse less since we have shards...
    def zranges(prefix: Long, offset: Int, oct: Long, level: Int): Unit = {
      val min: Long = prefix | (oct << offset) // QR + 00000...
      val max: Long = min | (1L << offset) - 1 // QR + 11111...
      val octRange = ZRange(n.apply(min), n.apply(max))

      if (searchRange.contains(octRange) || offset < 64 - precision) {
        // whole range matches, happy day
        mq += (octRange.min.z, octRange.max.z)
      } else if (searchRange.overlaps(octRange)) {
        if (level < maxRecurse && offset > 0) {
          // some portion of this range is excluded
          // let our children work on each subrange
          val nextOffset = offset - n.dims
          val nextLevel = level + 1
          var nextRegion = 0
          while (nextRegion < n.subRegions) {
            zranges(min, nextOffset, nextRegion, nextLevel)
            nextRegion += 1
          }
        } else {
          // bottom out - add the entire range so we don't miss anything
          mq += (octRange.min.z, octRange.max.z)
        }
      }
    }

    // kick off recursion over the narrowed space
    zranges(commonPrefix, 64 - commonBits, 0, 0)

    // return our aggregated results
    mq.toSeq
  }

  /**
   * Calculates the longest common binary prefix between two z longs
   *
   * @return (common prefix, number of bits in common)
   */
  def longestCommonPrefix(lower: Long, upper: Long, n: ZN): ZPrefix = {
    var bitShift = n.bits - n.dims
    while ((lower >>> bitShift) == (upper >>> bitShift) && bitShift > -1) {
      bitShift -= n.dims
    }
    bitShift += n.dims // increment back to the last valid value
    ZPrefix(lower & (Long.MaxValue << bitShift), 64 - bitShift)
  }

  case class ZPrefix(prefix: Long, precision: Int) // precision in bits
}
