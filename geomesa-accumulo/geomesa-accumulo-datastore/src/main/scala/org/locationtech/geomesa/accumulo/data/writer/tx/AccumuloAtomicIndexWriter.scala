/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer
package tx

import org.apache.accumulo.core.client.{ConditionalWriter, ConditionalWriterConfig}
import org.apache.accumulo.core.conf.ClientProperty
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException.ConditionalWriteStatus
import org.locationtech.geomesa.accumulo.data.writer.tx.MutationBuilder.{AppendBuilder, DeleteBuilder, UpdateBuilder}
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeatureType

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

/**
 * Accumulo atomic index writer implementation
 *
 * @param ds data store
 * @param sft simple feature type
 * @param indices indices to write to
 * @param wrapper feature wrapper
 * @param partition partition to write to (if partitioned schema)
 */
class AccumuloAtomicIndexWriter(
    ds: AccumuloDataStore,
    sft: SimpleFeatureType,
    indices: Seq[GeoMesaFeatureIndex[_, _]],
    wrapper: FeatureWrapper[WritableFeature],
    partition: Option[String]
  ) extends BaseIndexWriter[WritableFeature](indices, wrapper) {

  private val writers: Array[ConditionalWriter] = {
    val config = new ConditionalWriterConfig()
    config.setAuthorizations(ds.auths)
    val maxThreads = ClientProperty.BATCH_WRITER_THREADS_MAX.getInteger(ds.connector.properties())
    if (maxThreads != null) {
      config.setMaxWriteThreads(maxThreads)
    }
    val timeout = ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getTimeInMillis(ds.connector.properties())
    if (timeout != null) {
      config.setTimeout(timeout, TimeUnit.MILLISECONDS)
    }
    val tables = indices.map { index =>
      index.getTableNames(partition) match {
        case Seq(t) => t // should always be writing to a single table here
        case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
      }
    }
    tables.map(ds.connector.createConditionalWriter(_, config)).toArray
  }

  private val colFamilyMappings = indices.map(ColumnFamilyMapper.apply).toArray
  private val visCache = new VisibilityCache()

  override protected def append(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    val mutations = buildMutations(values, AppendBuilder.apply)
    val errors = mutate(mutations)
    if (errors.nonEmpty) {
      throw ConditionalWriteException(feature.feature.getID, errors)
    }
  }

  override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    val mutations = buildMutations(values, DeleteBuilder.apply)
    val errors = mutate(mutations)
    if (errors.nonEmpty) {
      throw ConditionalWriteException(feature.feature.getID, errors)
    }
  }

  override protected def update(
      feature: WritableFeature,
      values: Array[RowKeyValue[_]],
      previous: WritableFeature,
      previousValues: Array[RowKeyValue[_]]): Unit = {
    val mutations = Array.ofDim[Seq[MutationBuilder]](values.length)
    // note: these are temporary conditions - we update them below
    val updates = buildMutations(values, AppendBuilder.apply)
    val deletes = buildMutations(previousValues, DeleteBuilder.apply)
    var i = 0
    while (i < values.length) {
      val prev = ArrayBuffer(deletes(i): _*)
      val appends = updates(i).map { mutations =>
        // find any previous values that will be updated
        val p = prev.indexWhere(p => java.util.Arrays.equals(p.row, mutations.row))
        if (p == -1) { mutations } else {
          // note: side-effect
          // any previous values that aren't updated need to be deleted
          val toUpdate = prev.remove(p)
          val remaining = toUpdate.kvs.filterNot(kv => mutations.kvs.exists(_.equalKey(kv)))
          if (remaining.nonEmpty) {
            prev.append(toUpdate.copy(kvs = remaining))
          }
          // add the previous values as a condition on the write
          UpdateBuilder(mutations.row, mutations.kvs, toUpdate.kvs)
        }
      }
      mutations(i) = appends ++ prev
      i += 1
    }
    val errors = mutate(mutations)
    if (errors.nonEmpty) {
      throw ConditionalWriteException(feature.feature.getID, errors)
    }
  }

  private def mutate[T <: MutationBuilder](mutations: Array[Seq[T]]): Seq[ConditionalWriteStatus] = {
    val errors = ArrayBuffer.empty[ConditionalWriteStatus]
    val successes = ArrayBuffer.empty[(Int, MutationBuilder)]
    var i = 0
    while (i < mutations.length) {
      mutations(i).foreach { m =>
        val status = writers(i).write(m.apply()).getStatus
        if (status == ConditionalWriter.Status.ACCEPTED) {
          successes += i -> m
        } else {
          // TODO handle UNKNOWN and re-try?
//          val existingTx = WithClose(new IsolatedScanner(client.createScanner(table, Authorizations.EMPTY))) { scanner =>
//            scanner.setRange(new org.apache.accumulo.core.data.Range(new String(id, StandardCharsets.UTF_8)))
//            val iter = scanner.iterator()
//            if (iter.hasNext) {
//              val next = iter.next()
//              Some(next.getKey.getTimestamp -> next.getValue)
//            } else {
//              None
//            }
//          }
          val index = if (indices(i).attributes.isEmpty) { indices(i).name } else {
            s"${indices(i).name}:${indices(i).attributes.mkString(":")}"
          }
          errors += ConditionalWriteStatus(index, m.name, status)
        }
      }
      i += 1
    }
    if (errors.nonEmpty) {
      // revert any writes we made
      successes.foreach { case (i, m) =>
        writers(i).write(m.invert())
      }
    }
    errors.toSeq
  }

  private def buildMutations[T <: MutationBuilder](
      values: Array[RowKeyValue[_]],
      builderFactory: (Array[Byte], Seq[MutationValue]) => T): Array[Seq[T]] = {
    val mutations = Array.ofDim[Seq[T]](values.length)
    var i = 0
    while (i < values.length) {
      mutations(i) = values(i) match {
        case kv: SingleRowKeyValue[_] =>
          val values = kv.values.map { v =>
            MutationValue(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
          }
          Seq(builderFactory(kv.row, values))

        case mkv: MultiRowKeyValue[_] =>
          mkv.rows.map { row =>
            val values = mkv.values.map { v =>
              MutationValue(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
            }
            builderFactory(row, values)
          }
      }
      i += 1
    }
    mutations
  }

  override def flush(): Unit = {} // there is no batching here, every single write gets flushed

  override def close(): Unit = CloseQuietly(writers).foreach(e => throw e)
}
