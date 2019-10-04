
/**
@created on: 18/3/19,
@author: Shreesha N,
@version: v0.0.1
@system name: badgod
Description:

..todo::

*/

package com.project1.hadoop.queries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Query1 {
    /**
     * Write a job(s) that reports the customers whose Age between 20 and 50 (inclusive).
     */
    public static class CustomerCountMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        List<String> customerCount = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            customerCount = new ArrayList<>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String customerEntry = value.toString();
            String[] customerValues = customerEntry.split(",");
            int age = Integer.parseInt(customerValues[2]);
            if (age >= 20 && age <= 50) {
                // key - customer id
                // value - complete customer row
                context.write(new Text(customerValues[0]), new Text(customerEntry));
            }
        }

    }


    public static void main(String[] args) throws Exception {
        String inputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/customers.txt";
        String outputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/shree_output/query1/";

        Configuration conf = new Configuration();
        // add the below code if you are reading/writing from/to HDFS
        // String inputPath = "hdfs://localhost:9000/ds503/hw1/input/transactions.txt";
        // String outputPath = "hdfs://localhost:9000/ds503/hw1/output/query2/";
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));

        Job job = Job.getInstance(conf, "CustomerAgeBelow20Above50");
        job.setJarByClass(Query1.class);
        job.setMapperClass(Query1.CustomerCountMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
