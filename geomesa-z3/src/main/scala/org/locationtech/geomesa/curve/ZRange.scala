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

  /**
   * Returns (litmax, bigmin) for the given range and point
   */
  def zdivide(p: ZPoint, rmin: ZPoint, rmax: ZPoint, n: ZN): (ZPoint, ZPoint) = {
    val (litmax, bigmin) = zdiv(load(n), n.dims)(p.z, rmin.z, rmax.z)
    (n.apply(litmax), n.apply(bigmin))
  }

  /** Loads either 1000... or 0111... into starting at given bit index of a given dimension */
  private def load(n: ZN)(target: Long, p: Long, bits: Int, dim: Int): Long = {
    val mask = ~(Z3.split(n.maxValue >> (n.bits - bits)) << dim)
    val wiped = target & mask
    wiped | (n.split(p) << dim)
  }

  /**
   * Implements the the algorithm defined in: Tropf paper to find:
   * LITMAX: maximum z-index in query range smaller than current point, xd
   * BIGMIN: minimum z-index in query range greater than current point, xd
   *
   * @param load: function that knows how to load bits into appropraite dimension of a z-index
   * @param xd: z-index that is outside of the query range
   * @param rmin: minimum z-index of the query range, inclusive
   * @param rmax: maximum z-index of the query range, inclusive
   * @return (LITMAX, BIGMIN)
   */
  def zdiv(load: (Long, Long, Int, Int) => Long, dims: Int)(xd: Long, rmin: Long, rmax: Long): (Long, Long) = {
    require(rmin < rmax, s"min ($rmin) must be less than max ($rmax)")
    var zmin: Long = rmin
    var zmax: Long = rmax
    var bigmin: Long = 0L
    var litmax: Long = 0L

    def bit(x: Long, idx: Int) = {
      ((x & (1L << idx)) >> idx).toInt
    }
    def over(bits: Long)  = 1L << (bits - 1)
    def under(bits: Long) = (1L << (bits - 1)) - 1

    var i = 64
    while (i > 0) {
      i -= 1

      val bits = i/dims+1
      val dim  = i%dims

      ( bit(xd, i), bit(zmin, i), bit(zmax, i) ) match {
        case (0, 0, 0) =>
        // continue

        case (0, 0, 1) =>
          zmax   = load(zmax, under(bits), bits, dim)
          bigmin = load(zmin, over(bits), bits, dim)

        case (0, 1, 0) =>
        // sys.error(s"Not possible, MIN <= MAX, (0, 1, 0)  at index $i")

        case (0, 1, 1) =>
          bigmin = zmin
          return (litmax, bigmin)

        case (1, 0, 0) =>
          litmax = zmax
          return (litmax, bigmin)

        case (1, 0, 1) =>
          litmax = load(zmax, under(bits), bits, dim)
          zmin = load(zmin, over(bits), bits, dim)

        case (1, 1, 0) =>
        // sys.error(s"Not possible, MIN <= MAX, (1, 1, 0) at index $i")

        case (1, 1, 1) =>
        // continue
      }
    }
    (litmax, bigmin)
  }

  case class ZPrefix(prefix: Long, precision: Int) // precision in bits
}
