/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.kudu.data.{KuduDataStore, KuduDataStoreFactory}
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, PasswordParams}

/**
 * Abstract class for Kudu commands
 */
trait KuduDataStoreCommand extends DataStoreCommand[KuduDataStore] {

  override def params: KuduParams

  override def connection: Map[String, String] = {
    Map(
      KuduDataStoreFactory.Params.CatalogParam.getName     -> params.catalog,
      KuduDataStoreFactory.Params.KuduMasterParam.getName  -> params.master,
      KuduDataStoreFactory.Params.CredentialsParam.getName -> params.password
    ).filter(_._2 != null)
  }
}

object KuduDataStoreCommand {

  trait KuduParams extends CatalogParam with MasterParam with PasswordParams

  trait MasterParam {
    @Parameter(names = Array("-M", "--master"), description = "Kudu master server", required = true)
    var master: String = _
  }
}
