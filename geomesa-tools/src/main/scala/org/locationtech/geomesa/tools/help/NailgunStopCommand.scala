/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.help

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.help.NailgunStopCommand.NailgunStopParameters

/**
  * Note: this class is a placeholder for the 'ng-stop' function implemented in the 'geomesa-*' script, to get it
  * to show up in the JCommander help
  */
class NailgunStopCommand extends Command {
  override val name = "ng-stop"
  override val params = new NailgunStopParameters()
  override def execute(): Unit = {}
}

object NailgunStopCommand {
  @Parameters(commandDescription = "Stop the Nailgun server used for executing commands")
  class NailgunStopParameters {}
}

