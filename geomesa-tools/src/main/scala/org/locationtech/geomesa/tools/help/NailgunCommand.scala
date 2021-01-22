/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.help

import java.util.concurrent.TimeUnit

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.help.NailgunCommand._
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, Runner}

/**
 * Note: most of this class is a placeholder for the 'ng' functions implemented in the 'geomesa-*' script,
 * to get it to show up in the JCommander help. The stats command is implemented here
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

  @Parameters(commandDescription = "Display stats from the Nailgun server used for executing commands")
  class NailgunStatsParams {}

  class NailgunStatsCommand extends Command {

    import NailgunStatsCommand.{Header, Stats, Widths, toSeconds}

    import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter

    override val name: String = "stats"
    override val params: NailgunStatsParams = new NailgunStatsParams()

    override def execute(): Unit = {
      val widths = new Widths(Header)
      val stats = Runner.Timers.asMap().asScala.toSeq.sortBy(_._1).map { case (name, (active, timer)) =>
        val snap = timer.getSnapshot
        val stat =
          Stats(
            name,
            active.get.toString,
            timer.getCount.toString,
            f"${timer.getMeanRate * 60}%2.2f",
            f"${toSeconds(snap.getMean)}%2.2f",
            f"${toSeconds(snap.getMedian)}%2.2f",
            f"${toSeconds(snap.get95thPercentile)}%2.2f"
          )
        widths.update(stat)
        stat
      }
      widths += 2 // add comma+space delimiter

      Command.output.info(Header.padTo(widths))
      stats.foreach(s => Command.output.info(s.padTo(widths)))
    }
  }

  object NailgunStatsCommand {

    val DurationFactor: Long = TimeUnit.SECONDS.toNanos(1)
    val Header: Stats = Stats("command", "active", "complete", "rate/min", "average", "median", "95%")

    def toSeconds(duration: Double): Double = duration / DurationFactor

    case class Stats(
        name: String,
        active: String,
        complete: String,
        rate: String,
        mean: String,
        median: String,
        `95%`: String
      ) {
      def padTo(widths: Widths, delim: String = ","): String = {
        Seq(
          (name + delim).padTo(widths.name, ' '),
          (active + delim).padTo(widths.active, ' '),
          (complete + delim).padTo(widths.complete, ' '),
          (rate + delim).padTo(widths.rate, ' '),
          (mean + delim).padTo(widths.mean, ' '),
          (median + delim).padTo(widths.median, ' '),
          `95%` // don't need to pad final column
        ).mkString
      }
    }

    class Widths(
        var name: Int = 0,
        var active: Int = 0,
        var complete: Int = 0,
        var rate: Int = 0,
        var mean: Int = 0,
        var median: Int = 0,
        var `95%`: Int = 0
      ) {

      def this(stats: Stats) = {
        this()
        update(stats)
      }

      def +=(len: Int): Unit = {
        name     += len
        complete += len
        active   += len
        rate     += len
        mean     += len
        median   += len
        `95%`    += len
      }

      def update(stats: Stats): Unit = {
        name     = math.max(name, stats.name.length)
        complete = math.max(complete, stats.complete.length)
        active   = math.max(active, stats.active.length)
        rate     = math.max(rate, stats.rate.length)
        mean     = math.max(mean, stats.mean.length)
        median   = math.max(median, stats.median.length)
        `95%`    = math.max(`95%`, stats.`95%`.length)
      }
    }
  }

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

  @Parameters(commandDescription = "Displays the classpath of the Nailgun server used for executing commands")
  class NailgunClasspathParams {}

  class NailgunClasspathCommand extends Command {
    override val name: String = "classpath"
    override val params: NailgunClasspathParams = new NailgunClasspathParams()
    override def execute(): Unit = {}
  }
}

