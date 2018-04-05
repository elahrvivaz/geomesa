/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object ByteBuffers {

  implicit class RichByteBuffer(val bb: ByteBuffer) extends AnyVal {

    def toInputStream: InputStream = new ByteBufferInputStream(bb)

    def ensureRemaining(count: Int): ByteBuffer = {
      if (bb.remaining >= count) { bb } else {
        val expanded = ByteBuffer.allocate(bb.capacity() * 2)
        bb.flip()
        expanded.put(bb)
        expanded
      }
    }

    def putBytes(bytes: Array[Byte]): Unit = {
      bb.putInt(bytes.length)
      bb.put(bytes)
    }

    def getBytes: Array[Byte] = {
      val bytes = Array.ofDim[Byte](bb.getInt())
      bb.get(bytes)
      bytes
    }

    def getString: String = new String(getBytes, StandardCharsets.UTF_8)

    def toArray: Array[Byte] = {
      bb.flip()
      val bytes = Array.ofDim[Byte](bb.remaining)
      bb.get(bytes)
      bytes
    }
  }

  class ByteBufferInputStream(buffer: ByteBuffer) extends InputStream {

    override def read(): Int = {
      if (!buffer.hasRemaining) { -1 } else {
        buffer.get() & 0xFF
      }
    }

    override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
      if (!buffer.hasRemaining) { -1 } else {
        val read = math.min(length, buffer.remaining)
        buffer.get(bytes, offset, read)
        read
      }
    }
  }

}
