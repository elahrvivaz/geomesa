/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.utils.uuid

import java.security.SecureRandom
import java.util.UUID

import org.locationtech.geomesa.utils.cache.SoftThreadLocal

object TimeSortedUuidGenerator {

  private val r = new SecureRandom()
  private val timeBytes   = new SoftThreadLocal[Array[Byte]]
  private val randomBytes = new SoftThreadLocal[Array[Byte]]

  /**
   * Creates a UUID where the first 16 bytes are based on the current time and the second 16 bytes are
   * based on a random number. This should provide good uniqueness along with sorting by date
   * (useful for accumulo).
   */
  def createUuid(time: Long = System.currentTimeMillis()): String = {
    val mostSigBits = bytesToLong(timePart(time))
    val leastSigBits = bytesToLong(randomPart())
    new UUID(mostSigBits, leastSigBits).toString
  }

  /**
   * Creates the time based part of the uuid.
   */
  private def timePart(time: Long): Array[Byte] = {
    // get a reusable byte array
    val bytes = timeBytes.getOrElseUpdate(Array.ofDim[Byte](8))

    // write the time in a sorted fashion
    // we drop the 4 most significant bits as we need 4 bits extra for the version
    bytes(0) = (time >> 52).asInstanceOf[Byte]
    bytes(1) = (time >> 44).asInstanceOf[Byte]
    bytes(2) = (time >> 36).asInstanceOf[Byte]
    bytes(3) = (time >> 28).asInstanceOf[Byte]
    bytes(4) = (time >> 20).asInstanceOf[Byte]
    bytes(5) = (time >> 12).asInstanceOf[Byte]
    bytes(6) = ((time >> 8) & 0x0f).asInstanceOf[Byte]  // the 0x0f clears the version bits
    bytes(7) = time.asInstanceOf[Byte]

    // set the version number for the UUID
    bytes(6) = (bytes(6) | 0x40).asInstanceOf[Byte] // set to version 4 (designates a random uuid)

    bytes
  }

  /**
   * Creates the random part of the uuid.
   */
  private def randomPart(): Array[Byte] = {
    // get a reusable byte array
    val bytes = randomBytes.getOrElseUpdate(Array.ofDim[Byte](8))
    // set the random bytes
    r.nextBytes(bytes)
    // set the variant number for the UUID
    bytes(0) = (bytes(0) & 0x3f).asInstanceOf[Byte] // clear variant
    bytes(0) = (bytes(0) | 0x80).asInstanceOf[Byte] // set to IETF variant
    bytes
  }

  /**
   * Converts 8 bytes to a long
   */
  private def bytesToLong(bytes: Array[Byte]): Long = {
    var bits = 0L
    var i = 0
    while (i < 8) {
      bits = (bits << 8) | (bytes(i) & 0xff)
      i += 1
    }
    bits
  }
}
