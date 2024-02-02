package org.locationtech.geomesa.accumulo.data.index

import org.apache.accumulo.core.client.{ConditionalWriter, ConditionalWriterConfig}
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.data.{Condition, ConditionalMutation}
import org.apache.accumulo.core.security.ColumnVisibility
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.index.AccumuloAtomicIndexWriter.{ConditionMatcher, RowEntry}
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

/**
 * Accumulo index writer implementation
 *
 * @param ds data store
 * @param indices indices to write to
 * @param wrapper feature wrapper
 * @param partition partition to write to (if partitioned schema)
 */
class AccumuloAtomicIndexWriter(
    ds: AccumuloDataStore,
    indices: Seq[GeoMesaFeatureIndex[_, _]],
    wrapper: FeatureWrapper[WritableFeature],
    partition: Option[String]
  ) extends BaseIndexWriter[WritableFeature](indices, wrapper) {

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
    var i = 0
    val errors = new java.util.ArrayList[ConditionalWriteStatus]()
    while (i < values.length) {
      values(i) match {
        case kv: SingleRowKeyValue[_] =>
          val mutation = new ConditionalMutation(kv.row)
          kv.values.foreach { v =>
            val cf = colFamilyMappings(i)(v.cf)
            mutation.addCondition(new Condition(cf, v.cq)) // requires cf/cq to not exist
            mutation.put(cf, v.cq, visCache(v.vis), v.value)
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
              mutation.addCondition(new Condition(cf, v.cq)) // requires cf/cq to not exist
              mutation.put(cf, v.cq, visCache(v.vis), v.value)
            }
            val status = writers(i).write(mutation).getStatus
            if (status != ConditionalWriter.Status.ACCEPTED) {
              errors.add(ConditionalWriteStatus(indices(i).identifier, status))
            }
          }
      }
      i += 1
    }
    if (!errors.isEmpty) {
      throw ConditionalWriteException(feature.feature.getID, errors)
    }
  }

  override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    var i = 0
    val errors = new java.util.ArrayList[ConditionalWriteStatus]()
    while (i < values.length) {
      values(i) match {
        case SingleRowKeyValue(row, _, _, _, _, _, vals) =>
          val mutation = new ConditionalMutation(row)
          vals.foreach { v =>
            val cf = colFamilyMappings(i)(v.cf)
            val vis = visCache(v.vis)
            mutation.addCondition(new Condition(cf, v.cq).setValue(v.value).setVisibility(vis))
            mutation.putDelete(cf, v.cq, vis)
          }
          val status = writers(i).write(mutation).getStatus
          if (status != ConditionalWriter.Status.ACCEPTED) {
            errors.add(ConditionalWriteStatus(indices(i).identifier, status))
          }

        case MultiRowKeyValue(rows, _, _, _, _, _, vals) =>
          rows.foreach { row =>
            val mutation = new ConditionalMutation(row)
            vals.foreach { v =>
              val cf = colFamilyMappings(i)(v.cf)
              val vis = visCache(v.vis)
              mutation.addCondition(new Condition(cf, v.cq).setValue(v.value).setVisibility(vis))
              mutation.putDelete(cf, v.cq, vis)
            }
            val status = writers(i).write(mutation).getStatus
            if (status != ConditionalWriter.Status.ACCEPTED) {
              errors.add(ConditionalWriteStatus(indices(i).identifier, status))
            }
          }
      }
      i += 1
    }
    if (!errors.isEmpty) {
      throw ConditionalWriteException(feature.feature.getID, errors)
    }
  }

  override protected def update(
      feature: WritableFeature,
      values: Array[RowKeyValue[_]],
      previous: WritableFeature,
      previousValues: Array[RowKeyValue[_]]): Unit = {
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

  override def flush(): Unit = {}

  override def close(): Unit = CloseQuietly(writers).foreach(e => throw e)
}

object AccumuloAtomicIndexWriter {

  private class ConditionMatcher(entries: ArrayBuffer[RowEntry]) {
    def apply(row: Array[Byte], cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte]): Condition = {
      new Condition(cf, v.cq) // requires cf/cq to not exist
    }

    def remaining: Seq[RowEntry] = {
      ???
    }
  }

  private object ConditionMatcher {
    def apply(rkv: RowKeyValue[_], cfMapper: ColumnFamilyMapper, visCache: VisibilityCache): ConditionMatcher = {
      val entries = ArrayBuffer.empty[RowEntry]
      rkv match {
        case SingleRowKeyValue(row, _, _, _, _, _, vals) =>
          vals.foreach { v =>
            entries += RowEntry(row, cfMapper(v.cf), v.cq, visCache(v.vis), v.value)
          }

        case MultiRowKeyValue(rows, _, _, _, _, _, vals) =>
          rows.foreach { row =>
            vals.foreach { v =>
              entries += RowEntry(row, cfMapper(v.cf), v.cq, visCache(v.vis), v.value)
            }
          }
      }
      new ConditionMatcher(entries)
    }
  }

  private case class RowEntry(row: Array[Byte], cf: Array[Byte], cq: Array[Byte], vis: ColumnVisibility, value: Array[Byte])
}

