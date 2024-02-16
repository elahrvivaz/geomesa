/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer.tx

import org.apache.accumulo.core.client.{ConditionalWriter, ConditionalWriterConfig, IsolatedScanner}
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.data.{Condition, ConditionalMutation}
import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.util.TableUtils
import org.locationtech.geomesa.index.utils.Releasable
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.{ConcurrentModificationException, UUID}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object TransactionManager {
  def tableName(ds: AccumuloDataStore, typeName: String): String =
    s"${ds.config.catalog}_${StringSerialization.alphaNumericSafeString(typeName)}_tx"
}

/**
 *
 * @param ds data store
 * @param sft simple feature type being locked
 * @param timeout timeout waiting for a lock, in millis
 */
class TransactionManager(ds: AccumuloDataStore, sft: SimpleFeatureType, timeout: Long) {

  import ConditionalWriter.Status._

  private val client = ds.connector
  private val table = TransactionManager.tableName(ds, sft.getTypeName)
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
      Failure(new IllegalStateException(s"Could not acquire lock due to unexpected condition: $status"))
    } else { // REJECTED | UNKNOWN
      // for rejected, check to see if the lock has expired
      // for unknown, we need to scan the row to see if it was written or not
      val scanner = new IsolatedScanner(client.createScanner(table, Authorizations.EMPTY))
      val existingTx = try {
        scanner.setRange(new org.apache.accumulo.core.data.Range(new String(id, StandardCharsets.UTF_8)))
        val iter = scanner.iterator()
        if (iter.hasNext) {
          val next = iter.next()
          Some(next.getKey.getTimestamp -> next.getValue)
        } else {
          None
        }
      } finally {
        CloseWithLogging(scanner)
      }
      existingTx match {
        case None => lock(id) // mutation was rejected but row was subsequently deleted, or mutation failed - either way, re-try
        case Some((timestamp, existing)) =>
          if (timestamp < System.currentTimeMillis() + timeout) {
            if (existing.compareTo(txId) == 0) {
              Success(new Unlock(id))
            } else {
              Failure(new ConcurrentModificationException("Feature is locked for editing by another process"))
            }
          } else {
            val mutation = new ConditionalMutation(id)
            mutation.addCondition(new Condition(empty, empty).setTimestamp(timestamp).setValue(existing.get))
            mutation.put(empty, empty, txId)
            writer.write(mutation) // don't need to check the status... will re-try regardless
            lock(id)
          }
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
