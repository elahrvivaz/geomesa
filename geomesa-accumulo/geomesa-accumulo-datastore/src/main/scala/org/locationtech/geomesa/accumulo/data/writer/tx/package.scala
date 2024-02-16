/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer

import org.apache.accumulo.core.data.{Condition, ConditionalMutation}
import org.apache.accumulo.core.security.ColumnVisibility
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionBuilder.{AppendCondition, DeleteCondition, UpdateCondition}

package object tx {

  case class ConditionalMutations(row: Array[Byte], kvs: Seq[MutationValue], condition: ConditionBuilder) {

    /**
     * Create the mutation
     *
     * @return
     */
    def mutation(): ConditionalMutation = {
      val mutation = new ConditionalMutation(row)
      kvs.foreach { kv =>
        condition(kv.cf, kv.cq, kv.vis, kv.value).foreach(mutation.addCondition)
        condition.mutator(mutation, kv.cf, kv.cq, kv.vis, kv.value)
      }
      mutation
    }

    /**
     * Create a mutation that will undo the regular mutation from this object
     *
     * @return
     */
    def invert(): ConditionalMutation = {
      val inverted = condition match {
        case AppendCondition => copy(condition = DeleteCondition)
        case DeleteCondition => copy(condition = AppendCondition)
        case UpdateCondition(values) => ConditionalMutations(row, values, UpdateCondition(kvs))
      }
      inverted.mutation()
    }
  }

  case class MutationValue(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]) {
    def equalKey(other: MutationValue): Boolean = {
      java.util.Arrays.equals(cf, other.cf) &&
          java.util.Arrays.equals(cq, other.cq) &&
          java.util.Arrays.equals(vis.getExpression, other.vis.getExpression)
    }
  }

  sealed trait ConditionBuilder {
    def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition]
    def mutator: Mutator
  }

  object ConditionBuilder {
    object AppendCondition extends ConditionBuilder {
      override val mutator: Mutator = Mutator.Put
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition] =
        Seq(new Condition(cf, cq)) // requires cf+cq to not exist
    }

    object DeleteCondition extends ConditionBuilder {
      override val mutator: Mutator = Mutator.Delete
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition] =
        Seq(new Condition(cf, cq).setVisibility(vis).setValue(value))
    }

    case class UpdateCondition(previous: Seq[MutationValue]) extends ConditionBuilder {
      override val mutator: Mutator = Mutator.Put
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Seq[Condition] =
        previous.map(kv => new Condition(kv.cf, kv.cq).setVisibility(kv.vis).setValue(kv.value))
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
}
