/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands.stats

import com.beust.jcommander.{Parameter, ParameterException}
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.common.{FeatureTypeNameParam, OptionalCQLFilterParam}
import org.opengis.feature.simple.SimpleFeatureType

object StatsCommand {

  // gets attributes to run stats on, based on sft and input params
  def getAttributes(sft: SimpleFeatureType, params: AttributeStatsParams): Seq[String] = {
    import scala.collection.JavaConversions._

    if (params.attributes.isEmpty) {
      sft.getAttributeDescriptors.filter(GeoMesaStats.okForStats).map(_.getLocalName)
    } else {
      val descriptors = params.attributes.map(sft.getDescriptor)
      if (descriptors.contains(null)) {
        val invalid = params.attributes.zip(descriptors).filter(_._2 == null).map(_._1).mkString("', '")
        throw new ParameterException(s"Invalid attributes '$invalid' for schema ${sft.getTypeName}")
      }
      if (!descriptors.forall(GeoMesaStats.okForStats)) {
        val invalid = descriptors.filterNot(GeoMesaStats.okForStats)
            .map(d => s"${d.getLocalName}:${d.getType.getBinding.getSimpleName}").mkString("', '")
        throw new ParameterException(s"Can't evaluate stats for attributes '$invalid' due to unsupported data types")
      }
      params.attributes
    }
  }
}

trait StatsParams extends GeoMesaConnectionParams with FeatureTypeNameParam with OptionalCQLFilterParam

trait CachedStatsParams {
  @Parameter(names = Array("--no-cache"), description = "Calculate against the data set instead of using cached statistics (may be slow)")
  var exact: Boolean = false
}

trait AttributeStatsParams {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (use multiple flags or separate with commas)")
  var attributes: java.util.List[String] = new java.util.ArrayList[String]()
}
