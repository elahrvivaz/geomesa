package org.locationtech.geomesa.accumulo.data.writer

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.admin.NewTableConfiguration
import org.apache.accumulo.core.client.{AccumuloClient, AccumuloSecurityException, ConditionalWriter, ConditionalWriterConfig, IsolatedScanner}
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.data.{Condition, ConditionalMutation}
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.writer.AccumuloAtomicIndexWriter.ConditionBuilder.{NoCondition, TxCommitCondition, TxWriteCondition}
import org.locationtech.geomesa.accumulo.data.writer.AccumuloAtomicIndexWriter.{Mutator, TransactionManager, TxVisibilities}
import org.locationtech.geomesa.accumulo.util.TableUtils
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api.{RowKeyValue, _}
import org.locationtech.geomesa.index.utils.Releasable
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, WithClose}
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType

import java.nio.charset.StandardCharsets
import java.util.{ConcurrentModificationException, UUID}
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

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

  import AccumuloAtomicIndexWriter.ConditionBuilder._
  import AccumuloAtomicIndexWriter._

  private val locking = new TransactionManager(ds, sft)
  private val writerConfig: ConditionalWriterConfig = {
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
    config
  }

  private val writers = indices.toArray.map { index =>
    val table = index.getTableNames(partition) match {
      case Seq(t) => t // should always be writing to a single table here
      case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
    }
    ds.connector.createConditionalWriter(table, writerConfig)
  }
  private val colFamilyMappings = indices.map(ColumnFamilyMapper.apply).toArray
  private val visCache = new VisibilityCache()

  override protected def append(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    val lock = locking.lock(feature.id).get // re-throw any lock exception
    try {
      val errors = mutate(values, AppendCondition, Mutator.Put)
      if (!errors.isEmpty) {
        // clean up any mutations we wrote - since we use the same condition this should end up in the original state
        mutate(values, AppendCondition, Mutator.Delete)
        throw ConditionalWriteException(feature.feature.getID, errors)
      }
    } finally {
      lock.release()
    }
  }

  override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    val lock = locking.lock(feature.id).get // re-throw any lock exception
    try {
      val errors = mutate(values, DeleteCondition, Mutator.Delete)
      if (!errors.isEmpty) {
        // clean up any mutations we deleted - since we use the same condition this should end up in the original state
        mutate(values, DeleteCondition, Mutator.Put)
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
      // TODO match up previous/new values and update them without a separate delete
      var errors = mutate(previousValues, DeleteCondition, Mutator.Delete)
      if (!errors.isEmpty) {
        // clean up any mutations we deleted - since we use the same condition this should end up in the original state
        mutate(previousValues, DeleteCondition, Mutator.Put)
        throw ConditionalWriteException(feature.feature.getID, errors)
      }
      errors = mutate(values, AppendCondition, Mutator.Put)
      if (!errors.isEmpty) {
        // clean up any mutations we wrote - since we use the same condition this should end up in the original state
        mutate(values, AppendCondition, Mutator.Delete)
        throw ConditionalWriteException(feature.feature.getID, errors)
      }
    } finally {
      lock.release()
    }

    var i = 0
    val errors = new java.util.ArrayList[ConditionalWriteStatus]()
    while (i < values.length) {
      val matcher = ConditionMatcher(previousValues(i), colFamilyMappings(i), visCache)
      values(i) match {
        case kv: SingleRowKeyValue[_] =>
          // TODO need to delete any non-overwritten rows before writing the new ones so that we can recover in case of failure half-way
          val mutation = new ConditionalMutation(kv.row)
          kv.values.foreach { v =>
            val cf = colFamilyMappings(i)(v.cf)
            val vis = visCache(v.vis)
            mutation.addCondition(matcher(kv.row, cf, v.cq, vis, v.value))
            mutation.put(cf, v.cq, vis, v.value)
          }
          val status = writers(i).write(mutation).getStatus
          if (status != ConditionalWriter.Status.ACCEPTED) {
            errors.add(ConditionalWriteStatus(indices(i).identifier, status))
          }

        case mkv: MultiRowKeyValue[_] =>
          mkv.rows.foreach { row =>
            val mutation = new ConditionalMutation(row)
            mkv.values.foreach { v =>
              val cf = colFamilyMappings(i)(v.cf)
              val vis = visCache(v.vis)
              mutation.addCondition(matcher(row, cf, v.cq, vis, v.value))
              mutation.put(cf, v.cq, vis, v.value)
            }
            val status = writers(i).write(mutation).getStatus
            if (status != ConditionalWriter.Status.ACCEPTED) {
              errors.add(ConditionalWriteStatus(indices(i).identifier, status))
            }
          }
      }
      // delete any remaining entries that weren't overwritten
      matcher.remaining.foreach { case RowEntry(row, cf, cq, vis, value) =>
        val mutation = new ConditionalMutation(row)
        mutation.addCondition(new Condition(cf, cq).setValue(value).setVisibility(vis))
        mutation.putDelete(cf, cq, vis)
        val status = writers(i).write(mutation).getStatus
        if (status != ConditionalWriter.Status.ACCEPTED) {
          errors.add(ConditionalWriteStatus(indices(i).identifier, status))
        }
      }
      i += 1
    }
    if (!errors.isEmpty) {
      throw ConditionalWriteException(feature.feature.getID, errors)
    }
    ???
  }

  private def mutate(
      values: Array[RowKeyValue[_]],
      condition: ConditionBuilder,
      mutator: Mutator): java.util.List[ConditionalWriteStatus] = {
    val errors = new java.util.ArrayList[ConditionalWriteStatus]()
    var i = 0
    while (i < values.length) {
      values(i) match {
        case kv: SingleRowKeyValue[_] =>
          val mutation = new ConditionalMutation(kv.row)
          kv.values.foreach { v =>
            val cf = colFamilyMappings(i)(v.cf)
            val vis = visCache(v.vis)
            mutation.addCondition(condition(cf, v.cq, vis, v.value))
            mutator(mutation, cf, v.cq, vis, v.value)
          }
          val status = writers(i).write(mutation).getStatus
          if (status != ConditionalWriter.Status.ACCEPTED) {
            errors.add(ConditionalWriteStatus(indices(i).identifier, status))
          }

        case mkv: MultiRowKeyValue[_] =>
          mkv.rows.foreach { row =>
            val mutation = new ConditionalMutation(row)
            mkv.values.foreach { v =>
              val cf = colFamilyMappings(i)(v.cf)
              val vis = visCache(v.vis)
              mutation.addCondition(condition(cf, v.cq, vis, v.value))
              mutator(mutation, cf, v.cq, vis, v.value)
            }
            val status = writers(i).write(mutation).getStatus
            if (status != ConditionalWriter.Status.ACCEPTED) {
              errors.add(ConditionalWriteStatus(indices(i).identifier, status))
            }
          }
      }
      i += 1
    }
    errors
  }

  private def group(value: RowKeyValue[_], colFamilyMapper: ColumnFamilyMapper): Map[Row, Map[Key, Array[Byte]]] = {
    val entries = value match {
      case kv: SingleRowKeyValue[_] =>
        val internal = kv.values.map { v =>
          Key(colFamilyMapper(v.cf), v.cq, visCache(v.vis)) -> v.value
        }
        Seq(Row(kv.row) -> internal.toMap)

      case mkv: MultiRowKeyValue[_] =>
        mkv.rows.map { row =>
          val internal = mkv.values.map { v =>
            Key(colFamilyMapper(v.cf), v.cq, visCache(v.vis)) -> v.value
          }
          Row(row) -> internal.toMap
        }
    }
    entries.toMap
  }

  override def flush(): Unit = {} // there is no batching here, every single write gets flushed

  override def close(): Unit = CloseQuietly(writers).foreach(e => throw e)
}

object AccumuloAtomicIndexWriter extends LazyLogging {

  val LockTimeout: SystemProperty = SystemProperty("geomesa.accumulo.tx.lock.timeout", "30s")

  private class TransactionManager(ds: AccumuloDataStore, sft: SimpleFeatureType) {

    import ConditionalWriter.Status._

    private val client = ds.connector
    private val table = s"${ds.config.catalog}_${StringSerialization.alphaNumericSafeString(sft.getTypeName)}_tx"
    private val empty = Array.empty[Byte]
    private val txId = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
    private val timeout = LockTimeout.toDuration.get.toMillis // safe to .get since we have a valid default

    TableUtils.createTableIfNeeded(client, table, logical = false)

    private val writer = {
      val config = new ConditionalWriterConfig()
      config.setAuthorizations(ds.auths)
      val timeout = ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getTimeInMillis(client.properties())
      if (timeout != null) {
        config.setTimeout(timeout, TimeUnit.MILLISECONDS)
      }
      client.createConditionalWriter(table, config)
    }

    @tailrec
    final def lock(id: Array[Byte]): Try[Releasable] = {
      val mutation = new ConditionalMutation(id)
      mutation.addCondition(new Condition(empty, empty))
      mutation.put(empty, empty, txId)

      val status = writer.write(mutation).getStatus
      if (status == ACCEPTED) {
        Success(new Unlock(id))
      } else if (status == VIOLATED || status == INVISIBLE_VISIBILITY) {
        // the table shouldn't have any constraints and we're not writing with any visibilities
        throw new IllegalStateException(s"Could not acquire lock due to unexpected condition: $status")
      } else { // REJECTED | UNKNOWN
        // for rejected, check to see if the lock has expired
        // for unknown, we need to scan the row to see if it was written or not
        val scanner = new IsolatedScanner(client.createScanner(table, Authorizations.EMPTY))
        try {
          scanner.setRange(new org.apache.accumulo.core.data.Range(new String(id, StandardCharsets.UTF_8)))
          val iter = scanner.iterator()
          if (iter.hasNext) {
            val next = iter.next()
            val existingTx = next.getValue
            if (existingTx.compareTo(txId) == 0) {
              Success(new Unlock(id))
            } else if (next.getKey.getTimestamp < System.currentTimeMillis() + timeout) {
              Failure(new ConcurrentModificationException("Feature is locked for editing by another process"))
            } else {
              val mutation = new ConditionalMutation(id)
              val condition =
                new Condition(empty, empty).setTimestamp(next.getKey.getTimestamp).setValue(existingTx.get)
              mutation.addCondition(condition)
              mutation.put(empty, empty, txId)
              writer.write(mutation) // don't need to check the status... will re-try regardless
              lock(id)
            }
          } else {
            // mutation was rejected but row was subsequently deleted, or mutation failed - either way, re-try
            lock(id)
          }
        } finally {
          CloseWithLogging(scanner)
        }
      }
    }

    private class Unlock(id: Array[Byte]) extends Releasable {
      override def release(): Unit = {
        val mutation = new ConditionalMutation(id)
        mutation.addCondition(new Condition(empty, empty).setValue(txId))
        mutation.putDelete(empty, empty)

        val status = writer.write(mutation).getStatus
        if (status == UNKNOWN) {
          // for unknown, we need to scan the row to see if it was written or not
          val scanner = new IsolatedScanner(client.createScanner(table, Authorizations.EMPTY))
          try {
            scanner.setRange(new org.apache.accumulo.core.data.Range(new String(id, StandardCharsets.UTF_8)))
            val iter = scanner.iterator()
            if (iter.hasNext) {
              if (iter.next().getValue.compareTo(txId) == 0) {
                release() // re-try
              }
              // } else {
              // row was successfully deleted
            }
          } finally {
            CloseWithLogging(scanner)
          }
        } else if (status == VIOLATED || status == INVISIBLE_VISIBILITY) {
          // the table shouldn't have any constraints and we're not writing with any visibilities
          throw new IllegalStateException(s"Could not release lock due to unexpected condition: $status")
          // } else {
          //  ACCEPTED means lock was released
          //  REJECTED means lock was expired and overwritten
        }
      }
    }
  }

  private trait ConditionBuilder {
    def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Condition
  }

  private object ConditionBuilder {
    object AppendCondition extends ConditionBuilder {
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Condition =
        new Condition(cf, cq) // requires cf+cq to not exist
    }

    object DeleteCondition extends ConditionBuilder {
      override def apply(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Condition =
        new Condition(cf, cq).setVisibility(vis).setValue(value)
    }


  }

  private trait Mutator {
    def apply(
        mutation: ConditionalMutation,
        cf: Array[Byte],
        cq: Array[Byte],
        vis: ColumnVisibility,
        value: Array[Byte]): Unit
  }

  private object Mutator {
    object Put extends Mutator {
      override def apply(
          mutation: ConditionalMutation,
          cf: Array[Byte],
          cq: Array[Byte],
          vis: ColumnVisibility,
          value: Array[Byte]): Unit = mutation.put(cf, cq, vis, value)
    }
    object Delete extends Mutator {
      override def apply(
          mutation: ConditionalMutation,
          cf: Array[Byte],
          cq: Array[Byte],
          vis: ColumnVisibility,
          value: Array[Byte]): Unit = mutation.putDelete(cf, cq, vis)
    }
  }

  private case class Row(row: Array[Byte]) {
    override def equals(obj: Any): Boolean = {
      obj match {
        case Row(row) => java.util.Arrays.equals(row, this.row)
        case _ => false
      }
    }
    override def hashCode(): Int = java.util.Arrays.hashCode(row)
  }

  private case class Key(cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility) {
    override def equals(obj: Any): Boolean = {
      obj match {
        case Key(cf, cq, vis) =>
          java.util.Arrays.equals(cf, this.cf) && java.util.Arrays.equals(cq, this.cq) &&
              java.util.Arrays.equals(vis.getExpression, this.vis.getExpression)
        case _ => false
      }
    }
    override def hashCode(): Int =
      java.util.Arrays.hashCode(Array(cf, cq, vis.getExpression).map(java.util.Arrays.hashCode))
  }
}

