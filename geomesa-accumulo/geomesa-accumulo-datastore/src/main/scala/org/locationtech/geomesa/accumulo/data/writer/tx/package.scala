package org.locationtech.geomesa.accumulo.data.writer

import org.apache.accumulo.core.client.ConditionalWriter
import org.apache.accumulo.core.data.{Condition, ConditionalMutation}
import org.apache.accumulo.core.security.ColumnVisibility

package object tx {

  case class ConditionalMutations(
      row: Array[Byte],
      kvs: Seq[MutationValue],
      condition: ConditionBuilder,
      mutator: Mutator) {
    def mutation(): ConditionalMutation = {
      val mutation = new ConditionalMutation(row)
      kvs.foreach { kv =>
        condition(kv.cf, kv.cq, kv.vis, kv.value).foreach(mutation.addCondition)
        mutator(mutation, kv.cf, kv.cq, kv.vis, kv.value)
      }
      mutation
    }
    def invert(): ConditionalMutation = copy(mutator = mutator.invert).mutation()
  }

  case class MutationValue(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]) {
    def equalKey(other: MutationValue): Boolean = {
      java.util.Arrays.equals(cf, other.cf) &&
          java.util.Arrays.equals(cq, other.cq) &&
          java.util.Arrays.equals(vis.getExpression, other.vis.getExpression)
    }
  }

  trait ConditionBuilder {
    def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition]
  }

  object ConditionBuilder {
    object AppendCondition extends ConditionBuilder {
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition] =
        Seq(new Condition(cf, cq)) // requires cf+cq to not exist
    }

    object DeleteCondition extends ConditionBuilder {
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition] =
        Seq(new Condition(cf, cq).setVisibility(vis).setValue(value))
    }

    case class UpdateCondition(previous: Seq[MutationValue]) extends ConditionBuilder {
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition] = {
        new Condition(cf, cq).setVisibility(vis).setValue(value)
        ???
      }
    }
  }

  sealed trait Mutator {

    def apply(
        mutation: ConditionalMutation,
        cf: Array[Byte],
        cq: Array[Byte],
        vis: ColumnVisibility,
        value: Array[Byte]): Unit

    def invert: Mutator
  }

  object Mutator {
    object Put extends Mutator {
      override def invert: Mutator = Delete
      override def apply(
          mutation: ConditionalMutation,
          cf: Array[Byte],
          cq: Array[Byte],
          vis: ColumnVisibility,
          value: Array[Byte]): Unit = mutation.put(cf, cq, vis, value)
    }
    object Delete extends Mutator {
      override def invert: Mutator = Put
      override def apply(
          mutation: ConditionalMutation,
          cf: Array[Byte],
          cq: Array[Byte],
          vis: ColumnVisibility,
          value: Array[Byte]): Unit = mutation.putDelete(cf, cq, vis)
    }
  }

  class ConditionalWriteException(val fid: String, val rejections: java.util.List[ConditionalWriteStatus], msg: String)
      extends RuntimeException(msg)

  object ConditionalWriteException {

    import scala.collection.JavaConverters._

    def apply(fid: String, rejections: Seq[ConditionalWriteStatus]): ConditionalWriteException = {
      new ConditionalWriteException(fid, rejections.asJava,
        s"Conditional write was rejected for feature '$fid': ${rejections.mkString(", ")}")

    }
  }

  case class ConditionalWriteStatus(index: String, condition: ConditionalWriter.Status) {
    override def toString: String = s"$index:$condition"
  }
}
