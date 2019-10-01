package com.project2.outliercheck;

import com.project2.custominputformat.JSONInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutlierInXYSpace {
    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] values = value.toString().split(",");
            if (values.length == 2) {
                String flag = values[0].split("_")[1];
                String elevation = values[1].split("_")[1];

                // key - flag value
                // value - elevation value
                context.write(new Text(flag), new Text(elevation));
            }

        }
    }

    public static class CustomReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // input
            // key - flag value
            // value - list of elevation
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (Text i : values) {
                int elevation = Integer.parseInt(i.toString());
                if (elevation < min)
                    min = elevation;
                if (elevation > max)
                    max = elevation;
            }
            // output
            // key - flag
            // value - flag, max, min
            context.write(new Text(key), new Text(min + "," + max));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // add the below code if you are reading/writing from/to HDFS
//        String inputDataPath = "hdfs://localhost:9000/ds503/hw2/input/airfield.json";
//        String outputPath = "hdfs://localhost:9000/ds503/hw2/output/custominputformat/";

        String inputDataPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/airfield.json";
        String outputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/project2/custominputformat/";

//        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
//        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);
        Job job = Job.getInstance(conf, "OutlierInXYSpace");

        job.setJarByClass(OutlierInXYSpace.class);
        job.setMapperClass(OutlierInXYSpace.CustomMapper.class);
        job.setReducerClass(OutlierInXYSpace.CustomReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        JSONInputFormat.addInputPath(job, new Path(inputDataPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
