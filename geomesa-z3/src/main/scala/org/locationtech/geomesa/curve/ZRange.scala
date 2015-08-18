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
case class ZRange[T <: Product](min: ZPoint[T], max: ZPoint[T]) {

  // contains in user space - each dimension is contained
  def contains(r: ZRange[T]): Boolean = {
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
  def overlaps(r: ZRange[T]): Boolean = {
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
  def zranges[T <: Product](min: ZPoint[T], max: ZPoint[T], precision: Int = 64): Seq[(Long, Long)] = {
    val dims = min.dims

    val ZPrefix(commonPrefix, commonBits) = longestCommonPrefix(min.z, max.z, dims)

    val searchRange = ZRange(min, max)
    var mq = new MergeQueue // stores our results

    // base our recursion on the depth of the tree that we get 'for free' from the common prefix
    val maxRecurse = if (commonBits < 32) 7 else if (commonBits < 42) 6 else 5

    def zranges(prefix: Long, offset: Int, oct: Long, level: Int): Unit = {
      val min: Long = prefix | (oct << offset) // QR + 000...
      val max: Long = min | (1L << offset) - 1 // QR + 111...
      val octRange = ZRange(ZPoint.apply(min, dims).asInstanceOf[ZPoint[T]],
          ZPoint.apply(max, dims).asInstanceOf[ZPoint[T]])

      if (searchRange.contains(octRange) || offset < 64 - precision) {
        // whole range matches, happy day
        mq += (octRange.min.z, octRange.max.z)
      } else if (searchRange.overlaps(octRange)) {
        if (level < maxRecurse && offset > 0) {
          // some portion of this range is excluded
          // let our children work on each subrange
          val nextOffset = offset - dims
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
    zranges(commonPrefix, 64 - commonBits, 0, 0)

    // return our aggregated results
    mq.toSeq
  }

  /**
   * Calculates the longest common binary prefix between two z longs
   *
   * @return (common prefix, number of bits in common)
   */
  def longestCommonPrefix(lower: Long, upper: Long, dims: Int): ZPrefix = {
    var bitShift = 64 - dims
    while ((lower >>> bitShift) == (upper >>> bitShift) && bitShift > -1) {
      bitShift -= dims
    }
    bitShift += dims // increment back to the last valid value
    ZPrefix(lower & (Long.MaxValue << bitShift), 64 - bitShift)
  }

  case class ZPrefix(prefix: Long, precision: Int) // precision in bits
}