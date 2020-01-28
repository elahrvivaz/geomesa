/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.data

import com.google.cloud.bigtable.hbase.BigtableExtendedScan
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.locationtech.geomesa.hbase.data.HBaseIndexAdapter
import org.locationtech.geomesa.utils.index.ByteArrays

class BigtableIndexAdapter(ds: BigtableDataStore) extends HBaseIndexAdapter(ds) {

  import scala.collection.JavaConverters._

  // https://cloud.google.com/bigtable/quotas#limits-table-id
  override val tableNameLimit: Option[Int] = Some(50)

  override protected def configureScans(
      ranges: java.util.List[RowRange],
      small: Boolean,
      colFamily: Array[Byte],
      filters: Seq[HFilter],
      coprocessor: Boolean): Seq[Scan] = {

    if (filters.nonEmpty) {
      // bigtable does support some filters, but currently we only use custom filters that aren't supported
      throw new IllegalArgumentException(s"Bigtable doesn't support filters: ${filters.mkString(", ")}")
    }

    // check if these are large scans or small scans (e.g. gets)
    // only in the case of 'ID IN ()' queries will the scans be small
    // stopRowInclusive indicates a 'small' single row scan
    if (ranges.get(0).isStopRowInclusive) {
      ranges.asScala.map { r =>
        new Scan()
            .withStartRow(r.getStartRow, r.isStartRowInclusive)
            .withStopRow(r.getStopRow, r.isStopRowInclusive)
            .setSmall(true)
            .addFamily(colFamily)
      }
    } else {
      val scan = new BigtableExtendedScan()
      var start: Array[Byte] = ranges.get(0).getStartRow
      var stop: Array[Byte] = ranges.get(0).getStopRow
      ranges.asScala.foreach { r =>
        if (ByteArrays.ByteOrdering.lt(r.getStartRow, start)) {
          start = r.getStartRow
        }
        if (ByteArrays.ByteOrdering.gt(r.getStopRow, stop)) {
          stop = r.getStopRow
        }
        scan.addRange(r.getStartRow, r.getStopRow)
      }
      scan.addFamily(colFamily).withStartRow(start, true).withStopRow(stop, false)
      Seq(scan)
    }
  }
}
