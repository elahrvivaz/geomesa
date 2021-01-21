/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.help

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.help.NailgunCommand._
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands}

/**
  * Note: this class is a placeholder for the 'ng' functions implemented in the 'geomesa-*' script, to get it
  * to show up in the JCommander help
  */
class NailgunCommand extends CommandWithSubCommands {
  override val name: String = "ng"
  override val params: NailgunParams = new NailgunParams()
  override val subCommands: Seq[Command] =
    Seq(new NailgunStartCommand(), new NailgunStopCommand(), new NailgunStatsCommand(), new NailgunClasspathCommand())
}

object NailgunCommand {

  @Parameters(commandDescription = "Manage the Nailgun server used for executing commands")
  class NailgunParams {}

  @Parameters(commandDescription = "Start the Nailgun server used for executing commands")
  class NailgunStartParams {}

  class NailgunStartCommand extends Command {
    override val name: String = "start"
    override val params: NailgunStartParams = new NailgunStartParams()
    override def execute(): Unit = {}
  }

  @Parameters(commandDescription = "Stop the Nailgun server used for executing commands")
  class NailgunStopParams {}

  class NailgunStopCommand extends Command {
    override val name: String = "stop"
    override val params: NailgunStopParams = new NailgunStopParams()
    override def execute(): Unit = {}
  }

  @Parameters(commandDescription = "Display stats from the Nailgun server used for executing commands")
  class NailgunStatsParams {}

  class NailgunStatsCommand extends Command {
    override val name: String = "stats"
    override val params: NailgunStatsParams = new NailgunStatsParams()
    override def execute(): Unit = {}
  }

  @Parameters(commandDescription = "Displays the classpath of the Nailgun server used for executing commands")
  class NailgunClasspathParams {}

  class NailgunClasspathCommand extends Command {
    override val name: String = "classpath"
    override val params: NailgunClasspathParams = new NailgunClasspathParams()
    override def execute(): Unit = {}
  }
}

