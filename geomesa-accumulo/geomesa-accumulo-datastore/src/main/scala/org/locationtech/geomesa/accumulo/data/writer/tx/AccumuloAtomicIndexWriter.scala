package org.locationtech.geomesa.accumulo.data.writer
package tx

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{ConditionalWriter, ConditionalWriterConfig}
import org.apache.accumulo.core.conf.ClientProperty
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionBuilder.{AppendCondition, DeleteCondition, UpdateCondition}
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
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

  import AccumuloAtomicIndexWriter.LockTimeout

  // note: safe to call .get on LockTimeout as we have a default value
  private val locking = new TransactionManager(ds, sft, LockTimeout.toDuration.get.toMillis)

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
    val lock = locking.lock(feature.id).get // re-throw any lock exception
    try {
      val mutations = buildMutations(values, AppendCondition, Mutator.Put)
      val errors = mutate(mutations)
      if (errors.nonEmpty) {
        throw ConditionalWriteException(feature.feature.getID, errors)
      }
    } finally {
      lock.release()
    }
  }

  override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    val lock = locking.lock(feature.id).get // re-throw any lock exception
    try {
      val mutations = buildMutations(values, DeleteCondition, Mutator.Delete)
      val errors = mutate(mutations)
      if (errors.nonEmpty) {
        throw ConditionalWriteException(feature.feature.getID, errors)
      }
    } finally {
      lock.release()
    }
  }

  override protected def update(
      feature: WritableFeature,
      values: Array[RowKeyValue[_]],
      previous: WritableFeature,
      previousValues: Array[RowKeyValue[_]]): Unit = {
    val lock = locking.lock(feature.id).get // re-throw any lock exception
    try {
      val mutations = Array.ofDim[Seq[ConditionalMutations]](values.length)
      // note: these are temporary conditions - we update them below
      val updates = buildMutations(values, AppendCondition, Mutator.Put)
      val deletes = buildMutations(previousValues, DeleteCondition, Mutator.Delete)
      var i = 0
      while (i < values.length) {
        val prev = ArrayBuffer(deletes(i): _*)
        val appends = updates(i).map { mutations =>
          // find any previous values that will be updated
          val p = prev.indexWhere(p => java.util.Arrays.equals(p.row, mutations.row))
          if (p == -1) { mutations } else {
            // note: side-effect
            // any previous values that aren't updated need to be deleted
            val updated = prev.remove(i)
            val remaining = updated.kvs.filterNot(kv => mutations.kvs.exists(_.equalKey(kv)))
            if (remaining.nonEmpty) {
              prev.append(updated.copy(kvs = remaining))
            }
            // add the previous values as a condition on the write
            mutations.copy(condition = UpdateCondition(updated.kvs))
          }
        }
        mutations(i) = appends ++ prev
        i += 1
      }
      val errors = mutate(mutations)
      if (errors.nonEmpty) {
        throw ConditionalWriteException(feature.feature.getID, errors)
      }
    } finally {
      lock.release()
    }
  }

  private def mutate(mutations: Array[Seq[ConditionalMutations]]): Seq[ConditionalWriteStatus] = {
    val errors = ArrayBuffer.empty[ConditionalWriteStatus]
    var i = 0
    while (i < mutations.length) {
      mutations(i).foreach { m =>
        val status = writers(i).write(m.mutation()).getStatus
        if (status != ConditionalWriter.Status.ACCEPTED) {
          // TODO handle UNKNOWN and re-try?
          errors += ConditionalWriteStatus(indices(i).identifier, status)
        }
      }
      i += 1
    }
    if (errors.nonEmpty) {
      // revert any writes we made
      // condition is the same, so same mutations should be accepted/rejected, resulting in the original state
      i = 0
      while (i < mutations.length) {
        mutations(i).foreach(m => writers(i).write(m.invert()))
        i += 1
      }
    }
    errors.toSeq
  }

  private def buildMutations(
      values: Array[RowKeyValue[_]],
      condition: ConditionBuilder,
      mutator: Mutator): Array[Seq[ConditionalMutations]] = {
    val mutations = Array.ofDim[Seq[ConditionalMutations]](values.length)
    var i = 0
    while (i < values.length) {
      mutations(i) = values(i) match {
        case kv: SingleRowKeyValue[_] =>
          val values = kv.values.map { v =>
            MutationValue(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
          }
          Seq(ConditionalMutations(kv.row, values, condition, mutator))

        case mkv: MultiRowKeyValue[_] =>
          mkv.rows.map { row =>
            val values = mkv.values.map { v =>
              MutationValue(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
            }
            ConditionalMutations(row, values, condition, mutator)
          }
      }
      i += 1
    }
    mutations
  }

  override def flush(): Unit = {} // there is no batching here, every single write gets flushed

  override def close(): Unit = CloseQuietly(writers).foreach(e => throw e)
}

object AccumuloAtomicIndexWriter extends LazyLogging {

  val LockTimeout: SystemProperty = SystemProperty("geomesa.accumulo.tx.lock.timeout", "2s")
}

