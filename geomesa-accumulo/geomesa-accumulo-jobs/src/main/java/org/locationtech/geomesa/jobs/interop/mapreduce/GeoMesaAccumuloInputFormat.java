/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.interop.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat$;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Input format that will read simple features from GeoMesa based on a CQL query.
 * The key will be the feature ID. Configure using the static methods.
 */
public class GeoMesaAccumuloInputFormat extends InputFormat<Text, SimpleFeature> {

    private org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat delegate =
            new org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat();

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {
        return delegate.getSplits(context);
    }

    @Override
    public RecordReader<Text, SimpleFeature> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return delegate.createRecordReader(split, context);
    }

    @SuppressWarnings("unchecked")
    public static void configure(Job job, Map<String, String> dataStoreParams, Query query) {
        GeoMesaAccumuloInputFormat$.MODULE$.configure(job.getConfiguration(), dataStoreParams, query);
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    public static void configure(Job job,
                                 Map<String, String> dataStoreParams,
                                 String featureTypeName,
                                 String filter,
                                 String[] transform) {
        Filter f;
        try {
            f = filter == null ? Filter.INCLUDE : ECQL.toFilter(filter);
        } catch (CQLException e) {
            throw new RuntimeException(e);
        }
        String[] t = transform == null ? Query.ALL_NAMES : transform;
        GeoMesaAccumuloInputFormat$.MODULE$.configure(job.getConfiguration(), dataStoreParams, new Query(featureTypeName, f, t));
    }
}
