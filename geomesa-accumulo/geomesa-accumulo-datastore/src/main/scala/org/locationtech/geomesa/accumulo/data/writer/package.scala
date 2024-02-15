package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.ConditionalWriter
import org.apache.accumulo.core.security.ColumnVisibility

package object writer {

  import scala.collection.JavaConverters._

  trait VisibilityBuilder {
    def apply(vis: Array[Byte]): ColumnVisibility
  }

  /**
   * Cache for storing column visibilities - not thread safe
   */
  class VisibilityCache extends VisibilityBuilder {

    private val defaultVisibility = new ColumnVisibility()
    private val visibilities = new java.util.HashMap[VisHolder, ColumnVisibility]()

    def apply(vis: Array[Byte]): ColumnVisibility = {
      if (vis.isEmpty) { defaultVisibility } else {
        val lookup = new VisHolder(vis)
        var cached = visibilities.get(lookup)
        if (cached == null) {
          cached = new ColumnVisibility(vis)
          visibilities.put(lookup, cached)
        }
        cached
      }
    }
  }

  /**
    * Wrapper for byte array to use as a key in the cached visibilities map
    *
    * @param vis vis
    */
  private class VisHolder(val vis: Array[Byte]) {

    override def equals(other: Any): Boolean = other match {
      case that: VisHolder => java.util.Arrays.equals(vis, that.vis)
      case _ => false
    }

    override def hashCode(): Int = java.util.Arrays.hashCode(vis)
  }
}
