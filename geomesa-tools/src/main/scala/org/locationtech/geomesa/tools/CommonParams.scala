/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.util.regex.Pattern

import com.beust.jcommander.{Parameter, ParameterException}
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode

/**
  * Shared parameters as individual traits
  */

trait QueryParams extends CatalogParam with RequiredTypeNameParam with CqlFilterParam with OptionalAttributesParam

trait CatalogParam {
  @Parameter(names = Array("-c", "--catalog"), description = "Catalog table for GeoMesa datastore", required = true)
  var catalog: String = null
}

trait TypeNameParam {
  def featureName: String
}

trait RequiredTypeNameParam extends TypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = null
}

trait OptionalTypeNameParam extends TypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate")
  var featureName: String = null
}

trait FeatureSpecParam {
  def spec: String
}

trait RequiredFeatureSpecParam extends FeatureSpecParam {
  @Parameter(names = Array("-s", "--spec"),
    description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either", required = true)
  var spec: String = null
}

trait OptionalFeatureSpecParam extends FeatureSpecParam {
  @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either")
  var spec: String = null
}

trait CqlFilterParam {
  def cqlFilter: String
}

trait RequiredCqlFilterParam extends CqlFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", required = true)
  var cqlFilter: String = null
}

trait OptionalCqlFilterParam extends CqlFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate")
  var cqlFilter: String = null
}

trait OptionalDtgParam {
  @Parameter(names = Array("--dtg"), description = "DateTime field name to use as the default dtg")
  var dtgField: String = null
}

trait AttributesParam {
  def attributes: java.util.List[String]
}
trait OptionalAttributesParam extends AttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)")
  var attributes: java.util.List[String] = null
}

trait RequiredAttributesParam extends AttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)", required = true)
  var attributes: java.util.List[String] = null
}

trait OptionalSharedTablesParam {
  @Parameter(names = Array("--use-shared-tables"), description = "Use shared tables for feature storage (true/false)", arity = 1)
  var useSharedTables: Boolean = true //default to true in line with datastore
}

trait OptionalForceParam {
  @Parameter(names = Array("--force"), description = "Force execution without prompt")
  var force: Boolean = false
}

trait OptionalPatternParam {
  @Parameter(names = Array("--pattern"), description = "Regular expression for simple feature type names")
  var pattern: Pattern = null
}

trait OptionalIndexParam extends TypeNameParam {
  @Parameter(names = Array("--index"), description = "Specify a particular index to query", required = false)
  var index: String = null

  @throws[ParameterException]
  def loadIndex(ds: GeoMesaDataStore[_, _, _, _], mode: IndexMode): Option[GeoMesaFeatureIndex[_, _, _, _]] = {
    Option(index).filter(_.length > 0).map { name =>
      val untypedIndices = ds.manager.indices(ds.getSchema(featureName), mode)
      val indices =
        untypedIndices.asInstanceOf[Seq[GeoMesaFeatureIndex[_ <: GeoMesaDataStore[_, _, _, _], _ <: WrappedFeature, _, _]]]
      val matched = if (name.indexOf(':') != -1) {
        // full identifier with version
        indices.find(_.identifier.equalsIgnoreCase(name))
      } else {
        // just index name
        indices.find(_.name.equalsIgnoreCase(name))
      }
      matched.getOrElse {
        throw new ParameterException(s"Specified index ' $index' not found. " +
        s"Available indices are: ${indices.map(_.identifier).mkString(", ")}")
      }
    }
  }
}