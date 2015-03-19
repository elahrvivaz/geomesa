/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.interop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geomesa.feature.ScalaSimpleFeature;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes$;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FeatureWriterJob {

    public static class MyMapper extends Mapper<Text, SimpleFeature, Text, SimpleFeature> {

        static enum CountersEnum { FEATURES }

        Text text = new Text();
        SimpleFeatureType sft =
                SimpleFeatureTypes$.MODULE$.createType("test", "dtg:Date,*geom:Point:srid=4326");


        @Override
        public void map(Text key, SimpleFeature value, Context context)
                throws IOException, InterruptedException {
            Counter counter = context.getCounter(CountersEnum.class.getName(),
                                                 CountersEnum.FEATURES.toString());
            counter.increment(1);

            Object[] values = new Object[] { value.getAttribute("dtg"), value.getAttribute("geom") };
            SimpleFeature feature = new ScalaSimpleFeature(value.getID(), sft, values);
            context.write(text, feature);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "simple feature writer");

        job.setJarByClass(FeatureWriterJob.class);
        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(GeoMesaInputFormat.class);
        job.setOutputFormatClass(GeoMesaOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScalaSimpleFeature.class);

        Map<String, String> params = new HashMap<String, String>();
        params.put("instanceId", "pcloud2");
        params.put("zookeepers", "pzoo1,pzoo2,pzoo3");
        params.put("user", "root");
        params.put("password", "secret");
        params.put("tableName", "fr_catalog");

        String cql = "BBOX(geom, -165,5,-50,75) AND dtg DURING 2015-03-02T00:00:00.000Z/2015-03-02T23:59:59.999Z";

        GeoMesaInputFormat.configure(job, params, "fr", cql);
        GeoMesaOutputFormat.configureDataStore(job, params);

        job.getConfiguration().set("io.serializations",
                "org.apache.hadoop.io.serializer.WritableSerialization," +
                "org.locationtech.geomesa.jobs.mapreduce.SimpleFeatureSerialization");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
