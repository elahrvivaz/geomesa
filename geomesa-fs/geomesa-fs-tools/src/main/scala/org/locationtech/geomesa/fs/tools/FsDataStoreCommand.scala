/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import java.io.File
import java.net.{MalformedURLException, URL}
import java.util
import java.util.ServiceLoader

import com.beust.jcommander.{IValueValidator, Parameter, ParameterException}
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.FileSystemStorageFactory
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.tools.DataStoreCommand
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait FsDataStoreCommand extends DataStoreCommand[FileSystemDataStore] {

  override def params: FsParams

  override def connection: Map[String, String] = {
    FsDataStoreCommand.configureURLFactory()
    val url = try {
      if (params.path.matches("""\w+://.*""")) {
        new URL(params.path)
      } else {
        new File(params.path).toURI.toURL
      }
    } catch {
      case e: MalformedURLException => throw new ParameterException(s"Invalid URL ${params.path}: ", e)
    }
    Map(FileSystemStorageFactory.PathParam.getName -> url.toString,
      FileSystemStorageFactory.EncodingParam.getName -> params.encoding)
  }
}

object FsDataStoreCommand {

  private var urlStreamHandlerSet = false

  def configureURLFactory(): Unit = synchronized {
    if (!urlStreamHandlerSet) {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
      urlStreamHandlerSet = true
    }
  }

  trait FsParams extends PathParam with EncodingParam

  trait PathParam {
    @Parameter(names = Array("--path", "-p"), description = "Path to root of filesystem datastore", required = true)
    var path: String = _
  }

  // TODO future work would be nice to store this in metadata
  trait EncodingParam {
    // TODO csv???
    @Parameter(names = Array("--encoding", "-e"), description = "Encoding (parquet, orc, csv, etc)", required = true, validateValueWith = classOf[EncodingValidator])
    var encoding: String = _
  }

  trait PartitionParam {
    @Parameter(names = Array("--partitions"), description = "Partitions (if empty all partitions will be used)", required = false, variableArity = true)
    var partitions: java.util.List[String] = new util.ArrayList[String]()
  }

  trait SchemeParams {
    @Parameter(names = Array("--partition-scheme"), description = "PartitionScheme typesafe config string or file", required = true)
    var scheme: java.lang.String = _

    @Parameter(names = Array("--leaf-storage"), description = "Use Leaf Storage for Partition Scheme", required = false, arity = 1)
    var leafStorage: java.lang.Boolean = true

    @Parameter(names = Array("--storage-opt"), variableArity = true, description = "Additional storage opts (k=v)", required = false, converter = classOf[KeyValueConverter])
    var storageOpts: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()
  }

  class EncodingValidator extends IValueValidator[String] {
    override def validate(name: String, value: String): Unit = {
      import scala.collection.JavaConversions._
      val factories = ServiceLoader.load(classOf[org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory])
      val encodings = factories.iterator().map(_.encoding).toSeq.sorted
      if (!encodings.exists(_.equalsIgnoreCase(value))) {
        throw new ParameterException(s"$value is not a valid encoding for parameter $name." +
            s"Available encodings are: ${encodings.mkString(", ")}")
      }
    }
  }
}
