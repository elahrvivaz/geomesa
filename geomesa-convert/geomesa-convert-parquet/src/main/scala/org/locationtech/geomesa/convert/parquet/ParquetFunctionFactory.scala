/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import org.apache.avro.generic.GenericRecord
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroPath
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}
import org.locationtech.jts.geom.Coordinate

class ParquetFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(geometry)

  private val geometry = new AvroGeometryFn()

  private val gf = JTSFactoryFinder.getGeometryFactory

  class AvroGeometryFn extends NamedTransformerFunction(Seq("avroGeometry"), pure = true) {
    private var path: AvroPath = _
    override def getInstance: AvroGeometryFn = new AvroGeometryFn()
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (path == null) {
        path = AvroPath(args(1).asInstanceOf[String])
      }
      val record = path.eval(args(0).asInstanceOf[GenericRecord]).collect { case r: GenericRecord =>
        // TODO make these constants in parquet reader and handle other geom types
          for { x <- Option(r.get("x")); y <- Option(r.get("y")) } yield {
            gf.createPoint(new Coordinate(x.asInstanceOf[Double], y.asInstanceOf[Double]))
          }
      }
      record.flatten.orNull
    }
  }
}
