/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.{File, FilenameFilter}
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.ConvertCommand
import org.locationtech.geomesa.tools.export.formats.ExportFormats
import org.locationtech.geomesa.tools.export.formats.ExportFormats.ExportFormat
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConvertCommandTest extends Specification with LazyLogging {

  val csvInput = getClass.getResource("/convert/csv-data.csv").getFile
  val csvConf  = {
    val file = new File(getClass.getResource("/convert/csv-convert.conf").getFile)
    FileUtils.readFileToString(file, StandardCharsets.UTF_8)
  }

  val tsvInput = getClass.getResource("/convert/tsv-data.csv").getFile
  val tsvConf  = {
    val file = new File(getClass.getResource("/convert/tsv-convert.conf").getFile)
    FileUtils.readFileToString(file, StandardCharsets.UTF_8)
  }

  val jsonInput = getClass.getResource("/convert/json-data.json").getFile
  val jsonConf  = {
    val file = new File(getClass.getResource("/convert/json-convert.conf").getFile)
    FileUtils.readFileToString(file, StandardCharsets.UTF_8)
  }

  val inFormats = Seq(ExportFormats.Csv, ExportFormats.Tsv, ExportFormats.Json)
  val outFormats = ExportFormats.values.filter(_ != ExportFormats.Null).toSeq

  for (in <- inFormats; out <- outFormats) {
    logger.debug(s"Testing $in to $out converter")
    testPair(in, out)
  }

  def getInputFileAndConf(fmt: ExportFormat): (String, String) = {
    fmt match {
      case ExportFormats.Csv  => (csvInput,  csvConf)
      case ExportFormats.Tsv  => (tsvInput,  tsvConf)
      case ExportFormats.Json => (jsonInput, jsonConf)
    }
  }

  def testPair(inFmt: ExportFormat, outFmt: ExportFormat): Unit = {
    s"Convert Command should convert $inFmt -> $outFmt" in {
      val (inputFile, conf) = getInputFileAndConf(inFmt)
      val sft = CLArgResolver.getSft(conf)

      def withCommand[T](test: ConvertCommand => T): T = {
        val file = if (outFmt == ExportFormats.Leaflet) {
          File.createTempFile("convertTest", ".html")
        } else {
          File.createTempFile("convertTest", s".${outFmt.toString.toLowerCase}")
        }
        file.delete() // some output formats require that the file doesn't exist
        val command = new ConvertCommand
        command.params.files.add(inputFile)
        command.params.config = conf
        command.params.spec = conf
        command.params.outputFormat = outFmt
        command.params.file = file.getAbsolutePath

        try {
          test(command)
        } finally {
          if (!file.delete()) {
            file.deleteOnExit()
          }
          if (outFmt == ExportFormats.Shp) {
            val root = file.getName.takeWhile(_ != '.') + "."
            file.getParentFile.listFiles(new FilenameFilter() {
              override def accept(dir: File, name: String) = name.startsWith(root)
            }).foreach { file =>
              if (!file.delete()) {
                file.deleteOnExit()
              }
            }
          }
        }
      }

      "get a Converter" in {
        withCommand { command =>
          ConvertCommand.getConverter(command.params, sft) must not(beNull)
        }
      }
      "get an Exporter" in {
        withCommand { command =>
          WithClose(ConvertCommand.getExporter(command.params, ExportFormats.Csv, null))(_ must not(beNull))
        }
      }
      "convert File" in {
        withCommand { command =>
          val converter = ConvertCommand.getConverter(command.params, sft)
          val ec = converter.createEvaluationContext(Map("inputFilePath" -> inputFile))
          val files = Iterator.single(inputFile).flatMap(PathUtils.interpretPath)
          val features = ConvertCommand.convertFeatures(files, converter, ec, new Query())
          features.toSeq must haveLength(3)
        }
      }
      "export data" in {
        withCommand { command =>
          command.execute()
          new File(command.params.file).length() must beGreaterThan(0L)
        }
      }
    }
  }
}
