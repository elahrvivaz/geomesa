/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.{IOException, InputStreamReader}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}
import org.apache.hadoop.fs._
import org.locationtech.geomesa.fs.storage.api.{Metadata, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType

class FileMetadata private (fc: FileContext,
                            path: Path,
                            sft: SimpleFeatureType,
                            scheme: PartitionScheme,
                            encoding: String) extends Metadata with MethodProfiling with LazyLogging {

  import scala.collection.JavaConverters._

  private val partitions = new ConcurrentHashMap[String, java.util.Set[String]]()

  override def getSimpleFeatureType: SimpleFeatureType = sft

  override def getPartitionScheme: PartitionScheme = scheme

  override def getEncoding: String = encoding

  override def getPartitionCount: Int = partitions.size

  override def getFileCount: Int = {
    var count = 0
    partitions.asScala.foreach { case (_, files) => count += files.size() }
    count
  }

  override def getPartitions: java.util.List[String] = new java.util.ArrayList(partitions.keySet())

  override def getFiles(partition: String): java.util.List[String] =
    new java.util.ArrayList[String](partitions.get(partition))

  override def getPartitionFiles: java.util.Map[String, java.util.List[String]] = {
    val map = new java.util.HashMap[String, java.util.List[String]](partitions.size)
    partitions.asScala.foreach { case (k, v) =>
      map.put(k, new java.util.ArrayList(v))
    }
    map
  }

  override def setFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit = {
    val changed = partitions.size() != partitionsToFiles.size() ||
        partitionsToFiles.asScala.exists { case (partition, files) =>
          !Option(partitions.get(partition)).contains(new java.util.HashSet(files))
        }
    if (changed) {
      partitions.clear()
      partitionsToFiles.asScala.foreach { case (k, v) =>
        partitions.computeIfAbsent(k, FileMetadata.createSet).addAll(v)
      }
      FileMetadata.save(this, fc, path)
    }
  }

  override def addFile(partition: String, file: String): Unit = {
    if (partitions.computeIfAbsent(partition, FileMetadata.createSet).add(file)) {
      FileMetadata.save(this, fc, path)
    }
  }

  override def addFiles(partition: String, files: java.util.List[String]): Unit = {
    if (partitions.computeIfAbsent(partition, FileMetadata.createSet).addAll(files)) {
      FileMetadata.save(this, fc, path)
    }
  }

  override def addFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit = {
    var changed = false
    partitionsToFiles.asScala.foreach { case (k, v) =>
      changed = partitions.computeIfAbsent(k, FileMetadata.createSet).addAll(v) || changed
    }
    if (changed) {
      FileMetadata.save(this, fc, path)
    }
  }

  override def removeFile(partition: String, file: String): Unit = {
    if (Option(partitions.get(partition)).exists(_.remove(file))) {
      FileMetadata.save(this, fc, path)
    }
  }

  override def removeFiles(partition: String, files: java.util.List[String]): Unit = {
    if (Option(partitions.get(partition)).exists(_.removeAll(files))) {
      FileMetadata.save(this, fc, path)
    }
  }

  override def removeFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit = {
    var changed = false
    partitionsToFiles.asScala.foreach { case (k, v) =>
      changed = Option(partitions.get(k)).exists(_.removeAll(v)) || changed
    }
    if (changed) {
      FileMetadata.save(this, fc, path)
    }
  }

  override def replaceFiles(partition: String, files: java.util.List[String], replacement: String): Unit = {
    val removed = Option(partitions.get(partition)).exists(_.removeAll(files))
    val added = partitions.computeIfAbsent(partition, FileMetadata.createSet).add(replacement)
    if (removed || added) {
      FileMetadata.save(this, fc, path)
    }
  }
}

object FileMetadata extends MethodProfiling with LazyLogging {

  private val createSet = new java.util.function.Function[String, java.util.Set[String]] {
    override def apply(t: String): java.util.Set[String] =
      Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean])
  }

  private val createFlag = java.util.EnumSet.of(CreateFlag.CREATE)

  private val options = ConfigRenderOptions.concise().setFormatted(true)

  /**
    * Creates a new, empty metadata and persists it
    *
    * @param fc file system
    * @param path path to write metadata updates to
    * @param sft simple feature type
    * @param encoding fs encoding
    * @param scheme partition scheme
    * @return
    */
  def create(fc: FileContext,
             path: Path,
             sft: SimpleFeatureType,
             encoding: String,
             scheme: PartitionScheme): FileMetadata = {
    val metadata = new FileMetadata(fc, path, sft, scheme, encoding)
    save(metadata, fc, path)
    metadata
  }

  /**
    * Loads a metadata instance from an existing file
    *
    * @param fc file system
    * @param path path to the persisted metadata
    * @return
    */
  def load(fc: FileContext, path: Path): FileMetadata = {
    val config = profile {
      WithClose(new InputStreamReader(fc.open(path))) { in =>
        ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
      }
    } { case (_, time) => logger.trace(s"Loaded configuration in ${time}ms")}

    val sft = profile {
      SimpleFeatureTypes.createType(config.getConfig("featureType"), path = None)
    } { case (_, time) => logger.debug(s"Created SimpleFeatureType in ${time}ms")}

    // Load encoding
    val encoding = config.getString("encoding")

    // Load partition scheme - note we currently have to reload the SFT user data manually
    // which is why we have to add the partition scheme back to the SFT
    val scheme = PartitionScheme(sft, config.getConfig("partitionScheme"))
    PartitionScheme.addToSft(sft, scheme)

    val metadata = new FileMetadata(fc, path, sft, scheme, encoding)

    // Load Partitions
    profile {
      import scala.collection.JavaConverters._
      val partitionConfig = config.getConfig("partitions")
      partitionConfig.root().entrySet().asScala.foreach { e =>
        val key = e.getKey
        val set = createSet.apply(null)
        set.addAll(partitionConfig.getStringList(key))
        metadata.partitions.put(key, set)
      }
    } { case (_, time) => logger.debug(s"Loaded partitions in ${time}ms")}

    metadata
  }

  /**
    * Overwrites the persisted file on disk
    *
    * @param metadata metadata to persist
    * @param fc file context
    * @param path path to write to
    */
  private def save(metadata: FileMetadata, fc: FileContext, path: Path): Unit = metadata.synchronized {
    val config = profile {
      val sft = metadata.getSimpleFeatureType
      val sftConfig = SimpleFeatureTypes.toConfig(sft, includePrefix = false, includeUserData = true).root()
      ConfigFactory.empty()
        .withValue("featureType", sftConfig)
        .withValue("encoding", ConfigValueFactory.fromAnyRef(metadata.getEncoding))
        .withValue("partitionScheme", PartitionScheme.toConfig(metadata.getPartitionScheme).root())
        .withValue("partitions", ConfigValueFactory.fromMap(metadata.getPartitionFiles))
        .root
        .render(options)
    } { case(_, time) => logger.debug(s"Created config in ${time}ms") }

    Thread.sleep(1) // ensure that we have a unique nano time - nano time is only guaranteed to milli precision

    // write to a temporary file
    val tmp = path.suffix(s".tmp.${System.currentTimeMillis()}.${System.nanoTime()}")
    profile {
      WithClose(fc.create(tmp, createFlag, CreateOpts.createParent)) { out =>
        out.writeBytes(config)
        out.hflush()
        out.hsync()
      }
    } { case (_, time) => logger.debug(s"Wrote temp file in ${time}ms")}

    // rename the file to the actual file name
    // note: this *should* be atomic, according to
    // http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html#Atomicity
    profile {
      fc.rename(tmp, path, Rename.OVERWRITE)

      // Because of eventual consistency lets make sure they are there
      var tryNum = 0
      var done = false

      while (!done) {
        if (!fc.util().exists(tmp) && fc.util().exists(path)) {
          done = true
        } else if (tryNum < 4) {
          Thread.sleep((2 ^ tryNum) * 1000)
          tryNum += 1
        } else {
          throw new IOException(s"Unable to properly update metadata after $tryNum tries")
        }
      }
    } { case (_, time) => logger.debug(s"Renamed metadata file in ${time}ms")}

    // back up the file and delete any extra backups beyond 5
    profile {
      fc.util.copy(path, path.suffix(s".old.${System.currentTimeMillis()}.${System.nanoTime()}"))
    } { case (_, time) => logger.debug(s"Copied backup metadata file in ${time}ms")}

    profile {
      val backups = Option(fc.util.globStatus(path.suffix(".old.*"))).getOrElse(Array.empty)

      // Keep the 5 most recent metadata files and delete the old ones
      backups.sortBy(_.getPath.getName)(Ordering.String.reverse).drop(5).foreach { backup =>
        logger.debug(s"Removing old metadata backup $backup")
        fc.delete(backup.getPath, false)
      }

      backups
    } { case (files, time) => logger.debug(s"Deleted ${math.max(0, files.length - 5)} old backup files in ${time}ms")}
  }
}