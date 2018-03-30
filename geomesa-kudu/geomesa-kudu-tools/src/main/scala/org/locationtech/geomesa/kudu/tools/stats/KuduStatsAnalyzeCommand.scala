/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.{KuduParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.kudu.tools.stats.KuduStatsAnalyzeCommand.KuduStatsAnalyzeParams
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.stats.{StatsAnalyzeCommand, StatsAnalyzeParams}

class KuduStatsAnalyzeCommand extends StatsAnalyzeCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduStatsAnalyzeParams
}

object KuduStatsAnalyzeCommand {
  @Parameters(commandDescription = "Analyze statistics on a GeoMesa feature type")
  class KuduStatsAnalyzeParams extends StatsAnalyzeParams with KuduParams
      with RequiredTypeNameParam with ToggleRemoteFilterParam
}
