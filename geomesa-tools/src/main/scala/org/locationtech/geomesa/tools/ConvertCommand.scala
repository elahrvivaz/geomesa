/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.File
import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.metadata.NoOpMetadata
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.LocalQueryRunner.ArrowDictionaryHook
import org.locationtech.geomesa.index.stats.MetadataBackedStats
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.{ExportParams, createOutputStream, createWriter, ensureOutputFile}
import org.locationtech.geomesa.tools.export.formats.ExportFormats.ExportFormat
import org.locationtech.geomesa.tools.export.formats.FileSystemExporter.{OrcFileSystemExporter, ParquetFileSystemExporter}
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, SeqStat, Stat}
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ConvertCommand extends Command with MethodProfiling with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {
    def complete(count: Option[Long], time: Long): Unit =
      Command.user.info(s"Conversion complete to ${Option(params.file).getOrElse("standard out")} " +
          s"in ${time}ms${count.map(c => s" for $c features").getOrElse("")}")

    profile(complete _)(convertAndExport())
  }

  private def convertAndExport(): Option[Long] = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val files = if (!params.files.isEmpty) { params.files.iterator.asScala.flatMap(PathUtils.interpretPath) } else {
      StdInHandle.available().map(Iterator.single).getOrElse {
        throw new ParameterException("Missing option: <files>... is required")
      }
    }

    val sft = CLArgResolver.getSft(params.spec)

    Command.user.info(s"Using SFT definition: ${SimpleFeatureTypes.encodeType(sft)}")

    val format = ExportCommand.getOutputFormat(params)
    val query = ExportCommand.createQuery(sft, format, params)

    val converter = ConvertCommand.getConverter(params, sft)
    val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(""))

    val exporter = ConvertCommand.getExporter(params, format, query.getHints)

    try {
      exporter.start(query.getHints.getReturnSft)
      val count = WithClose(ConvertCommand.convertFeatures(files, converter, ec, query))(exporter.export)
      val records = ec.counter.getLineCount - (if (params.noHeader) { 0 } else { params.files.size })
      Command.user.info(s"Converted ${getPlural(records, "line")} "
          + s"with ${getPlural(ec.counter.getSuccess, "success", "successes")} "
          + s"and ${getPlural(ec.counter.getFailure, "failure")}")
      count
    } finally {
      CloseWithLogging(exporter)
      CloseWithLogging(converter)
    }
  }
}

object ConvertCommand extends LazyLogging {

  def getConverter(params: ConvertParameters, sft: SimpleFeatureType): SimpleFeatureConverter = {
    val converterConfig = if (params.config != null) {
      CLArgResolver.getConfig(params.config)
    } else {
      throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
    }
    SimpleFeatureConverter(sft, converterConfig)
  }

  /**
    * Get an exporter
    *
    * @param params parameters
    * @param format export format
    * @param hints query hints
    * @return
    */
  def getExporter(params: ConvertParameters, format: ExportFormat, hints: Hints): FeatureExporter = {

    import ExportFormats._

    lazy val avroCompression = if (params.gzip == null) { Deflater.DEFAULT_COMPRESSION } else {
      val compression = params.gzip.toInt
      params.gzip = null // disable compressing the output stream, as it's handled by the avro writer
      compression
    }

    format match {
      case Arrow          => new ArrowExporter(hints, createOutputStream(params), Map.empty)
      case Avro           => new AvroExporter(avroCompression, createOutputStream(params))
      case Bin            => new BinExporter(hints, createOutputStream(params))
      case Csv            => DelimitedExporter.csv(createWriter(params), !params.noHeader, includeIds = true)
      case GeoJson | Json => new GeoJsonExporter(createWriter(params))
      case Gml | Xml      => new GmlExporter(createOutputStream(params))
      case Html | Leaflet => new LeafletMapExporter(params)
      case Null           => NullExporter
      case Orc            => new OrcFileSystemExporter(ensureOutputFile(params, format))
      case Parquet        => new ParquetFileSystemExporter(ensureOutputFile(params, format))
      case Shp            => new ShapefileExporter(new File(ensureOutputFile(params, format)))
      case Tsv            => DelimitedExporter.tsv(createWriter(params), !params.noHeader, includeIds = true)
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _ => throw new UnsupportedOperationException(s"Format $format can't be exported")
    }
  }

  def convertFeatures(
      files: Iterator[FileHandle],
      converter: SimpleFeatureConverter,
      ec: EvaluationContext,
      query: Query): CloseableIterator[SimpleFeature] = {

    import EvaluationContext.RichEvaluationContext
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    def convert(): CloseableIterator[SimpleFeature] = CloseableIterator(files).flatMap { file =>
      ec.setInputFilePath(file.path)
      val is = PathUtils.handleCompression(file.open, file.path)
      converter.process(is, ec)
    }

    def filter(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
      if (query.getFilter == Filter.INCLUDE) { iter } else { iter.filter(query.getFilter.evaluate) }

    def limit(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
      if (query.isMaxFeaturesUnlimited) { iter } else { iter.take(query.getMaxFeatures) }

    def transform(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val stats = new MetadataBackedStats(null, new NoOpMetadata[Stat], false) {
        override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, ignored: Filter): Seq[T] = {
          val stat = Stat(sft, stats)
          try {
            WithClose(limit(filter(convert())))(_.foreach(stat.observe))
            stat match {
              case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
              case s => Seq(s).asInstanceOf[Seq[T]]
            }
          } catch {
            case e: Exception =>
              logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(ignored)}'", e)
              Seq.empty
          }
        }
        override protected def write(typeName: String, stats: Seq[MetadataBackedStats.WritableStat]): Unit = {}
      }
      val hook = Some(ArrowDictionaryHook(stats, Option(query.getFilter).filter(_ != Filter.INCLUDE)))
      LocalQueryRunner.transform(converter.targetSft, iter, query.getHints.getTransform, query.getHints, hook)
    }

    transform(limit(filter(convert())))
  }

  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends ExportParams with InputFilesParam with OptionalTypeNameParam
      with RequiredFeatureSpecParam with RequiredConverterConfigParam
}