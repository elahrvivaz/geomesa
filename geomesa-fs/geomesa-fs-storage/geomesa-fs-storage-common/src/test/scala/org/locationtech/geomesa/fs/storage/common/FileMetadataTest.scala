/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.File
import java.nio.file.Files
import java.util.Collections

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path, RemoteIterator}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class FileMetadataTest extends Specification with AllExpectations {

  import scala.collection.JavaConverters._

  lazy val fc = FileContext.getFileContext(new Configuration())
  val sft = SimpleFeatureTypes.createType("metadata", "name:String,dtg:Date,*geom:Point:srid=4326")
  val encoding = "parquet"
  val scheme = CommonSchemeLoader.build("hourly,z2-2bit", sft)

  "FileMetadata" should {
    "create and persist an empty metadata file" in {
      withPath { path =>
        val created = FileMetadata.create(fc, path, sft, encoding, scheme)
        val loaded = FileMetadata.load(fc, path)
        foreach(Seq(created, loaded)) { metadata =>
          metadata.getEncoding mustEqual encoding
          metadata.getSimpleFeatureType mustEqual sft
          metadata.getPartitionScheme mustEqual scheme
          metadata.getPartitionCount mustEqual 0
          metadata.getFileCount mustEqual 0
          metadata.getPartitions.asScala must beEmpty
          metadata.getPartitionFiles.asScala must beEmpty
        }
      }
    }
    "persist file changes" in {
      withPath { path =>
        val created = FileMetadata.create(fc, path, sft, encoding, scheme)
        created.addFile("1", "file1")
        created.addFiles("1", java.util.Arrays.asList("file2", "file3"))
        created.addFiles(Map("1" -> Collections.singletonList("file4"), "2" -> java.util.Arrays.asList("file5", "file6")).asJava)
        val loaded = FileMetadata.load(fc, path)
        foreach(Seq(created, loaded)) { metadata =>
          metadata.getEncoding mustEqual encoding
          metadata.getSimpleFeatureType mustEqual sft
          metadata.getPartitionScheme mustEqual scheme
          metadata.getPartitionCount mustEqual 2
          metadata.getPartitions.asScala must containTheSameElementsAs(Seq("1", "2"))
          metadata.getFileCount mustEqual 6
          metadata.getFiles("1").asScala must containTheSameElementsAs((1 to 4).map(i => s"file$i"))
          metadata.getFiles("2").asScala must containTheSameElementsAs((5 to 6).map(i => s"file$i"))
          metadata.getPartitionFiles.asScala.keys must containTheSameElementsAs(Seq("1", "2"))
          metadata.getPartitionFiles.get("1").asScala must containTheSameElementsAs((1 to 4).map(i => s"file$i"))
          metadata.getPartitionFiles.get("2").asScala must containTheSameElementsAs((5 to 6).map(i => s"file$i"))
        }
      }
    }
    "keep back files" in {
      // noinspection LanguageFeature
      implicit def remoteIterToIter[T](iter: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): T = iter.next
      }

      withPath { path =>
        val metadata = FileMetadata.create(fc, path, sft, encoding, scheme)
        fc.util.listFiles(path.getParent, false).filter(_.getPath.getName != path.getName).toSeq must haveLength(1)
        metadata.addFile("1", "file1")
        fc.util.listFiles(path.getParent, false).filter(_.getPath.getName != path.getName).toSeq must haveLength(2)
        metadata.addFiles("1", java.util.Arrays.asList("file2", "file3"))
        fc.util.listFiles(path.getParent, false).filter(_.getPath.getName != path.getName).toSeq must haveLength(3)
        metadata.addFiles(Map("1" -> Collections.singletonList("file4"), "2" -> java.util.Arrays.asList("file5", "file6")).asJava)

        // backups, sorted by oldest first
        val backups = fc.util.listFiles(path.getParent, false).toSeq.collect {
          case p if p.getPath.getName != path.getName => p.getPath
        }.sortBy(_.toString)(Ordering.String).map(FileMetadata.load(fc, _))

        backups must haveLength(4)

        foreach(backups) { backup =>
          backup.getEncoding mustEqual encoding
          backup.getSimpleFeatureType mustEqual sft
          backup.getPartitionScheme mustEqual scheme
        }

        backups.head.getFileCount mustEqual 0
        backups(1).getFileCount mustEqual 1
        backups(2).getFileCount mustEqual 3
        backups(3).getFileCount mustEqual 6

        // ensure at most 5 backups are kept - create 3 new modifications
        (7 until 10).foreach(i => metadata.addFile("1", s"file$i"))

        // backups, sorted by oldest first
        val mostRecent = fc.util.listFiles(path.getParent, false).toSeq.collect {
          case p if p.getPath.getName != path.getName => p.getPath
        }.sortBy(_.toString)(Ordering.String).map(FileMetadata.load(fc, _))

        mostRecent must haveLength(5)

        foreach(mostRecent) { backup =>
          backup.getEncoding mustEqual encoding
          backup.getSimpleFeatureType mustEqual sft
          backup.getPartitionScheme mustEqual scheme
        }

        mostRecent.head.getFileCount mustEqual 3
        mostRecent.last.getFileCount mustEqual 9
      }
    }
  }

  def withPath[R](code: (Path) => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile.getPath
    try {
      code(new Path(file, MetadataFileSystemStorage.MetadataFileName))
    } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }
}
