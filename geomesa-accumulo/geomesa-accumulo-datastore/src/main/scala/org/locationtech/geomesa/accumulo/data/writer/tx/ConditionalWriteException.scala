package org.locationtech.geomesa.accumulo.data.writer.tx

import org.apache.accumulo.core.client.ConditionalWriter
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException.ConditionalWriteStatus
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter.WriteException

/**
 * Exception for conditional write failures
 *
 * @param fid feature id
 * @param rejections failed mutations
 * @param msg exception message
 */
class ConditionalWriteException(fid: String, rejections: java.util.List[ConditionalWriteStatus], msg: String)
    extends WriteException(msg) {
  def getFeatureId: String = fid
  def getRejections: java.util.List[ConditionalWriteStatus] = rejections
}

object ConditionalWriteException {

  import scala.collection.JavaConverters._

  def apply(fid: String, rejections: Seq[ConditionalWriteStatus]): ConditionalWriteException = {
    new ConditionalWriteException(fid, rejections.asJava,
      s"Conditional write was rejected for feature '$fid': ${rejections.mkString(", ")}")
  }

  case class ConditionalWriteStatus(index: String, action: String, condition: ConditionalWriter.Status) {
    def getIndex: String = index
    def getAction: String = action
    def getCondition: ConditionalWriter.Status = condition
    override def toString: String = s"$index $action $condition"
  }
}
