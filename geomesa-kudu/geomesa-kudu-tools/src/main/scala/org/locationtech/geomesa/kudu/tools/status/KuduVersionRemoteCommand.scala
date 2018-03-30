/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.{KuduParams, RemoteFilterNotUsedParam}
import org.locationtech.geomesa.kudu.tools.status.KuduVersionRemoteCommand.KuduVersionParams
import org.locationtech.geomesa.tools.status.VersionRemoteCommand

class KuduVersionRemoteCommand extends VersionRemoteCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduVersionParams
}

object KuduVersionRemoteCommand {
  @Parameters(commandDescription = "Display the GeoMesa version installed on a cluster")
  class KuduVersionParams extends KuduParams with RemoteFilterNotUsedParam
}
