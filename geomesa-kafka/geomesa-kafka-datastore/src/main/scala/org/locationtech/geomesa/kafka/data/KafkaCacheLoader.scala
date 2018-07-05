/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{FeatureEvent, FeatureListener}
import org.locationtech.geomesa.kafka.KafkaConsumerVersions
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.kafka.utils.{GeoMessageSerializer, KafkaFeatureEvent}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

/**
  * Reads from Kafka and populates a `KafkaFeatureCache`.
  * Manages geotools feature listeners
  */
trait KafkaCacheLoader extends Closeable with LazyLogging {

  import scala.collection.JavaConverters._

  private val listeners = {
    val map = new ConcurrentHashMap[(SimpleFeatureSource, FeatureListener), java.lang.Boolean]()
    Collections.newSetFromMap(map).asScala
  }

  // use a flag instead of checking listeners.isEmpty, which is slightly expensive for ConcurrentHashMap
  @volatile
  private var hasListeners = false

  def cache: KafkaFeatureCache

  def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = synchronized {
    listeners.add((source, listener))
    hasListeners = true
  }

  def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = synchronized {
    listeners.remove((source, listener))
    hasListeners = listeners.nonEmpty
  }

  protected [KafkaCacheLoader] def fireEvent(message: Change, timestamp: Long): Unit = {
    if (hasListeners) {
      fireEvent(KafkaFeatureEvent.changed(_, message.feature, timestamp))
    }
  }

  protected [KafkaCacheLoader] def fireEvent(message: Delete, timestamp: Long): Unit = {
    if (hasListeners) {
      val removed = cache.query(message.id).orNull
      fireEvent(KafkaFeatureEvent.removed(_, message.id, removed, timestamp))
    }
  }

  protected [KafkaCacheLoader] def fireEvent(message: Clear, timestamp: Long): Unit = {
    if (hasListeners) {
      fireEvent(KafkaFeatureEvent.cleared(_, timestamp))
    }
  }

  private def fireEvent(toEvent: (SimpleFeatureSource) => FeatureEvent): Unit = {
    val events = scala.collection.mutable.Map.empty[SimpleFeatureSource, FeatureEvent]
    listeners.foreach { case (source, listener) =>
      val event = events.getOrElseUpdate(source, toEvent(source))
      try { listener.changed(event) } catch {
        case NonFatal(e) => logger.error(s"Error in feature listener for $event", e)
      }
    }
  }
}

object KafkaCacheLoader {

  object NoOpLoader extends KafkaCacheLoader {
    override val cache: KafkaFeatureCache = KafkaFeatureCache.empty()
    override def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def close(): Unit = {}
  }

  class KafkaCacheLoaderImpl(sft: SimpleFeatureType,
                             override val cache: KafkaFeatureCache,
                             override protected val consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
                             override protected val topic: String,
                             override protected val frequency: Long,
                             lazyDeserialization: Boolean,
                             private var doInitialLoad: Boolean) extends ThreadedConsumer with KafkaCacheLoader {

    private val serializer = new GeoMessageSerializer(sft, lazyDeserialization)

    try { classOf[ConsumerRecord[Any, Any]].getMethod("timestamp") } catch {
      case e: NoSuchMethodException => logger.warn("This version of Kafka doesn't support timestamps, using system time")
    }

    if (doInitialLoad) {
      doInitialLoad = false
      // for the initial load, don't bother indexing in the feature cache until we have the final state
      val executor = Executors.newSingleThreadExecutor()
      executor.submit(new InitialLoader(consumers, topic, frequency, serializer, this))
      executor.shutdown()
    } else {
      startConsumers()
    }

    override def close(): Unit = {
      try {
        super.close()
      } finally {
        cache.close()
      }
    }

    override protected [KafkaCacheLoader] def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      val message = serializer.deserialize(record.key(), record.value())
      val timestamp = try { record.timestamp() } catch { case e: NoSuchMethodError => System.currentTimeMillis() }
      logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
      message match {
        case m: Change => fireEvent(m, timestamp); cache.put(m.feature)
        case m: Delete => fireEvent(m, timestamp); cache.remove(m.id)
        case m: Clear  => fireEvent(m, timestamp); cache.clear()
        case m => throw new IllegalArgumentException(s"Unknown message: $m")
      }
    }
  }

  /**
    * Handles initial loaded 'from-beginning' without indexing features in the spatial index. Will still
    * trigger message events.
    *
    * @param consumers consumers, won't be closed even on call to 'close()'
    * @param topic kafka topic
    * @param frequency polling frequency in milliseconds
    * @param serializer message serializer
    * @param toLoad main cache loader, used for callback when bulk loading is done
    */
  private class InitialLoader(override protected val consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
                              override protected val topic: String,
                              override protected val frequency: Long,
                              serializer: GeoMessageSerializer,
                              toLoad: KafkaCacheLoaderImpl) extends ThreadedConsumer with Runnable {

    private val loadCache = new ConcurrentHashMap[String, SimpleFeature](1000)

    // track the offsets that we want to read to
    private val offsets = new ConcurrentHashMap[Int, Long]()
    private val done = new AtomicBoolean(false)

    override protected def closeConsumers: Boolean = false

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (done.get) { toLoad.consume(record) } else {
        val message = serializer.deserialize(record.key, record.value)
        val timestamp = try { record.timestamp() } catch { case e: NoSuchMethodError => System.currentTimeMillis() }
        logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
        message match {
          case m: Change => toLoad.fireEvent(m, timestamp); loadCache.put(m.feature.getID, m.feature)
          case m: Delete => toLoad.fireEvent(m, timestamp); loadCache.remove(m.id)
          case m: Clear  => toLoad.fireEvent(m, timestamp); loadCache.clear()
          case m => throw new IllegalArgumentException(s"Unknown message: $m")
        }
        // once we've hit the max offset for the partition, remove from the offset map to indicate we're done
        if (offsets.getOrDefault(record.partition, Long.MaxValue) <= record.offset) {
          offsets.remove(record.partition)
        }
      }
    }

    override def run(): Unit = {
      import scala.collection.JavaConverters._

      val partitions = consumers.head.partitionsFor(topic).asScala.map(_.partition)
      try {
        // note: these methods are not available in kafka 0.9, which will cause it to fall back to normal loading
        val beginningOffsets = KafkaConsumerVersions.beginningOffsets(consumers.head, topic, partitions)
        val endOffsets = KafkaConsumerVersions.endOffsets(consumers.head, topic, partitions)
        partitions.foreach { p =>
          // end offsets are the *next* offset that will be returned, so subtract one to track the last offset
          // we will actually consume
          val endOffset = endOffsets.getOrElse(p, 0L) - 1L
          // note: not sure if start offsets are also off by one, but at the worst we would skip bulk loading
          // for the last message per topic
          val beginningOffset = beginningOffsets.getOrElse(p, 0L)
          if (beginningOffset < endOffset) {
            offsets.put(p, endOffset)
          }
        }
      } catch {
        case e: NoSuchMethodException => logger.warn(s"Can't support initial bulk loading for current Kafka version: $e")
      }
      // don't bother spinning up the consumer threads if we don't need to actually bulk load anything
      if (!offsets.isEmpty) {
        startConsumers() // kick off the asynchronous consumer threads
        try {
          // offsets will get removed from the map in `consume`, so we're done when the map is empty
          while (!offsets.isEmpty) {
            Thread.sleep(1000)
          }
        } finally {
          // stop the consumer threads, but won't close the consumers due to `closeConsumers`
          close()
        }
        // set a flag just in case the consumer threads haven't finished spinning down, so that we will
        // pass any additional messages back to the main loader
        done.set(true)
        loadCache.asScala.foreach { case (_, v) => toLoad.cache.put(v) }
      }
      // start the normal loading
      toLoad.startConsumers()
    }
  }
}

