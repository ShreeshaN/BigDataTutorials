/**
 * @created on: 18/3/19,
 * @author: Shreesha N,
 * @version: v0.0.1
 * @system name: badgod
 * Description:
 * <p>
 * ..todo::
 */

package com.project2.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomInputFormat {
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

        if (args.length < 2) {
            throw new Exception("Pass all the required arguments. Input file and Output filepath");
        }

        Configuration conf = new Configuration();

        String inputDataPath = args[0];
        String outputPath = args[1];

        // add the below code if you are reading/writing from/to HDFS
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null) {
            throw new Exception("HADOOP_HOME not found. Please make sure system path has HADOOP_HOME point to hadoop installation directory");
        }
        conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);
        Job job = Job.getInstance(conf, "CustomInputFormat");

        job.setJarByClass(CustomInputFormat.class);
        job.setMapperClass(CustomInputFormat.CustomMapper.class);
        job.setReducerClass(CustomInputFormat.CustomReducer.class);

        job.setInputFormatClass(JSONInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        JSONInputFormat.addInputPath(job, new Path(inputDataPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        if (fs != null)
            fs.close();
    }
}
