/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import java.io.InputStream

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.locationtech.geomesa.convert.avro.AvroConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicFieldConvert, BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.parquet.io.SimpleFeatureParquetSchema
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

class ParquetConverterFactory
    extends AbstractConverterFactory[ParquetConverter, BasicConfig, BasicField, BasicOptions] {

  import scala.collection.JavaConverters._

  override protected val typeToProcess: String = ParquetConverterFactory.TypeToProcess

  override protected implicit def configConvert: ConverterConfigConvert[BasicConfig] = BasicConfigConvert
  override protected implicit def fieldConvert: FieldConvert[BasicField] = BasicFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert

  override def infer(is: InputStream, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] =
    infer(is, sft, None)

  /**
    * Handles parquet files (including those produced by the FSDS and CLI export)
    *
    * @param is input
    * @param sft simple feature type, if known ahead of time
    * @param path file path, if there is a file available
    * @return
    */
  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] = {

    try {
      is.close() // we don't use the input stream, just close it

      path.flatMap { p =>
        val conf = new Configuration()
        val footer = ParquetFileReader.readFooter(conf, new Path(p), ParquetMetadataConverter.NO_FILTER)
        val (schema, fields, id) = SimpleFeatureParquetSchema.read(footer.getFileMetaData) match {
          case Some(SimpleFeatureParquetSchema(dataSft, _)) =>
            // this is a geomesa encoded parquet file
            val fields = dataSft.getAttributeDescriptors.asScala.map { descriptor =>
              // note: parquet converter stores the generic record under index 0
              val path = s"avroPath($$0, '/${SimpleFeatureParquetSchema.name(descriptor)}')"
              // some types need a function applied to the underlying avro value
              val expression = ObjectType.selectType(descriptor).head match {
                case ObjectType.DATE     => s"millisToDate($path)"
                case ObjectType.UUID     => s"avroBinaryUuid($path)"
                case ObjectType.GEOMETRY => s"avroGeometry($path)"
                case _                   => path
              }
              BasicField(descriptor.getLocalName, Some(Expression(expression)))
            }
            val id = Expression(s"avroPath($$0, '/${SimpleFeatureParquetSchema.FeatureIdField}')")

            // validate the existing schema, if any
            if (sft.exists(_.getAttributeDescriptors.asScala != dataSft.getAttributeDescriptors.asScala)) {
              throw new IllegalArgumentException("Inferred schema does not match existing schema")
            }
            (dataSft, fields, Some(id))

          case _ =>
            // this is an arbitrary parquet file, create fields based on the schema
            val messageType = footer.getFileMetaData.getSchema
            val types = AvroConverterFactory.schemaTypes(new AvroSchemaConverter(conf).convert(messageType))
            val dataSft = TypeInference.schema("inferred-parquet", types)
            // note: parquet converter stores the generic record under index 0
            val fields = types.map(t => BasicField(t.name, Some(Expression(t.transform.apply(0)))))

            // validate the existing schema, if any
            sft.foreach(AbstractConverterFactory.validateInferredType(_, types.map(_.typed)))

            (dataSft, fields, None)
        }

        val converterConfig = BasicConfig(typeToProcess, id, Map.empty, Map.empty)

        val config = configConvert.to(converterConfig)
            .withFallback(fieldConvert.to(fields))
            .withFallback(optsConvert.to(BasicOptions.default))
            .toConfig

        Some((schema, config))
      }
    } catch {
      case NonFatal(e) => logger.debug(s"Could not infer Parquet converter from input:", e); None
    }
  }
}

object ParquetConverterFactory {
  val TypeToProcess = "parquet"
}
