package org.locationtech.geomesa.accumulo.data.writer.tx

import org.apache.accumulo.core.client.{ConditionalWriter, ConditionalWriterConfig, IsolatedScanner}
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.data.{Condition, ConditionalMutation}
import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.writer.AccumuloAtomicIndexWriter.LockTimeout
import org.locationtech.geomesa.accumulo.util.TableUtils
import org.locationtech.geomesa.index.utils.Releasable
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType

import java.nio.charset.StandardCharsets
import java.util.{ConcurrentModificationException, UUID}
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}


/**
 *
 * @param ds data store
 * @param sft simple feature type being locked
 * @param timeout timeout waiting for a lock, in millis
 */
private class TransactionManager(ds: AccumuloDataStore, sft: SimpleFeatureType, timeout: Long) {

  import ConditionalWriter.Status._

  private val client = ds.connector
  private val table = s"${ds.config.catalog}_${StringSerialization.alphaNumericSafeString(sft.getTypeName)}_tx"
  private val empty = Array.empty[Byte]
  private val txId = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)

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