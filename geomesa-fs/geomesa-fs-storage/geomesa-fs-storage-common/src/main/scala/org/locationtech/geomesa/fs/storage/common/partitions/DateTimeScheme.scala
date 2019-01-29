/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.{Collections, Date, Optional}

import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues}
import org.locationtech.geomesa.fs.storage.api.{FilterPartitions, PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.annotation.tailrec

class DateTimeScheme(fmtStr: String,
                     stepUnit: ChronoUnit,
                     step: Int,
                     dtg: String,
                     leaf: Boolean) extends PartitionScheme {

  private val MinDateTime = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  private val MaxDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)

  private val fmt = DateTimeFormatter.ofPattern(fmtStr)

  override def getName: String = DateTimeScheme.Name

  override def getPartition(feature: SimpleFeature): String =
    fmt.format(feature.getAttribute(dtg).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC))

  override def getPartitions(filter: Filter): Optional[java.util.List[FilterPartitions]] = {
    val bounds = FilterHelper.extractIntervals(filter, dtg, handleExclusiveBounds = true)
    if (bounds.disjoint) {
      Optional.of(Collections.emptyList())
    } else if (bounds.isEmpty) {
      Optional.empty()
    } else {
      val covered = new java.util.ArrayList[String]()
      val partial = new java.util.ArrayList[String]()
      getPartitions(bounds).foreach { start =>
        val end = start.plus(step, stepUnit)
        val partition = Bounds(Bound(Some(start), inclusive = true), Bound(Some(end), inclusive = false))

        @tailrec
        def check(intervals: Iterator[Bounds[ZonedDateTime]]): Option[java.util.ArrayList[String]] = {
          val b = intervals.next()
          if (b.covers(partition)) {
            Some(covered)
          } else if (b.intersects(partition)) {
            Some(partial)
          } else if (!intervals.hasNext) {
            None
          } else {
            check(intervals)
          }
        }

        check(bounds.values.iterator).foreach(_.add(fmt.format(start)))
      }

      def coveredFilter: Filter = {
        import org.locationtech.geomesa.filter.{andOption, isTemporalFilter, partitionSubFilters}
        andOption(partitionSubFilters(filter, isTemporalFilter(_, dtg))._2).getOrElse(Filter.INCLUDE)
      }

      if (covered.isEmpty && partial.isEmpty) {
        Optional.of(Collections.emptyList()) // equivalent to Filter.EXCLUDE
      } else if (covered.isEmpty) {
        Optional.of(Collections.singletonList(new FilterPartitions(filter, partial, false)))
      } else if (partial.isEmpty) {
        Optional.of(Collections.singletonList(new FilterPartitions(coveredFilter, covered, false)))
      } else {
        val filterPartitions = new java.util.ArrayList[FilterPartitions](2)
        filterPartitions.add(new FilterPartitions(coveredFilter, covered, false))
        filterPartitions.add(new FilterPartitions(filter, partial, false))
        Optional.of(filterPartitions)
      }
    }
  }

  // TODO This may not be the best way to calculate max depth...
  // especially if we are going to use other separators
  override def getMaxDepth: Int = fmtStr.count(_ == '/')

  override def isLeafStorage: Boolean = leaf

  override def getOptions: java.util.Map[String, String] = {
    import DateTimeScheme.Config._

    import scala.collection.JavaConverters._

    Map(
      DtgAttribute      -> dtg,
      DateTimeFormatOpt -> fmtStr,
      StepUnitOpt       -> stepUnit.toString,
      StepOpt           -> step.toString,
      LeafStorage       -> leaf.toString
    ).asJava
  }

  private def getPartitions(bounds: FilterValues[Bounds[ZonedDateTime]]): Seq[ZonedDateTime] = {
    val intervals = bounds.values.map(b => (b.lower.value.getOrElse(MinDateTime), b.upper.value.getOrElse(MaxDateTime)))
    val partitions = intervals.flatMap { case (start, end) =>
      val count = stepUnit.between(start, end).toInt + 1
      Seq.tabulate(count)(i => start.plus(step * i, stepUnit))
    }
    partitions.distinct
  }

  override def equals(other: Any): Boolean = other match {
    case that: DateTimeScheme => that.getOptions.equals(getOptions)
    case _ => false
  }

  override def hashCode(): Int = getOptions.hashCode()
}

object DateTimeScheme {

  val Name = "datetime"

  object Config {
    val DateTimeFormatOpt: String = "datetime-format"
    val StepUnitOpt      : String = "step-unit"
    val StepOpt          : String = "step"
    val DtgAttribute     : String = "dtg-attribute"
    val LeafStorage      : String = LeafStorageConfig
  }

  object Formats {

    private val all = Seq(Minute, Hourly, Daily, Weekly, Monthly, JulianMinute, JulianHourly, JulianDaily)

    def apply(name: String): Option[Format] = all.find(_.name.equalsIgnoreCase(name))

    sealed case class Format private [Formats] (name: String, format: String, unit: ChronoUnit)

    object Minute       extends Format("minute",        "yyyy/MM/dd/HH/mm", ChronoUnit.MINUTES)
    object Hourly       extends Format("hourly",        "yyyy/MM/dd/HH",    ChronoUnit.HOURS  )
    object Daily        extends Format("daily",         "yyyy/MM/dd",       ChronoUnit.DAYS   )
    object Weekly       extends Format("weekly",        "yyyy/ww",          ChronoUnit.WEEKS  )
    object Monthly      extends Format("monthly",       "yyyy/MM",          ChronoUnit.MONTHS )
    object JulianMinute extends Format("julian-minute", "yyyy/DDD/HH/mm",   ChronoUnit.MINUTES)
    object JulianHourly extends Format("julian-hourly", "yyyy/DDD/HH",      ChronoUnit.HOURS  )
    object JulianDaily  extends Format("julian-daily",  "yyyy/DDD",         ChronoUnit.DAYS   )
  }

  class DateTimePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(name: String,
                      sft: SimpleFeatureType,
                      options: java.util.Map[String, String]): Optional[PartitionScheme] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      lazy val leaf = Option(options.get(Config.LeafStorage)).forall(java.lang.Boolean.parseBoolean)
      lazy val step = Option(options.get(Config.StepOpt)).map(_.toInt).getOrElse(1)
      lazy val dtg = {
        val field = Option(options.get(Config.DtgAttribute)).orElse(sft.getDtgField).getOrElse {
          throw new IllegalArgumentException(s"DateTime scheme requires valid attribute '${Config.DtgAttribute}'")
        }
        if (sft.indexOf(field) == -1) {
          throw new IllegalArgumentException(s"Attribute '$field' does not exist in simple feature type ${sft.getTypeName}")
        }
        field
      }

      if (name == Name) {
        val unit = Option(options.get(Config.StepUnitOpt)).map(c => ChronoUnit.valueOf(c.toUpperCase)).getOrElse {
          throw new IllegalArgumentException(s"DateTime scheme requires valid unit '${Config.StepUnitOpt}'")
        }
        val format = Option(options.get(Config.DateTimeFormatOpt)).getOrElse {
          throw new IllegalArgumentException(s"DateTime scheme requires valid format '${Config.DateTimeFormatOpt}'")
        }
        require(!format.endsWith("/"), "Format cannot end with a slash")

        Optional.of(new DateTimeScheme(format, unit, step, dtg, leaf))
      } else {
        import org.locationtech.geomesa.utils.conversions.JavaConverters._
        Formats(name).map(f => new DateTimeScheme(f.format, f.unit, step, dtg, leaf)).asJava
      }
    }
  }
}