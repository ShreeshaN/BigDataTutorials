/**
 * @created on: 8/4/19,
 * @author: Shreesha N,
 * @version: v0.0.1
 * @system name: badgod
 * Description:
 * <p>
 * ..todo::
 */

package com.project2.outliercheck;

import com.project2.custominputformat.JSONInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


// doubts
// 1. if i load cache, will it be distributed to all mappers ? So in that case it is ok to maintain global variables
//          and set them in setup of mapper ?
// 2. if i have 4 nodes, how to merge the output written by reducer into one single file ?
//          (outputformatter will just format output on node level,right ?)


public class OutlierInXYSpace {

    public static double radius;
    public static int thresholdK;


    public static class CustomMapper
            extends Mapper<Object, Text, IntWritable, Text> {
        private int xRange;
        private int yRange;
        private double radius;
        private int dividerX;
        private int dividerY;
        List<SubSpace> subSpaceList;
        int divisions;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            xRange = Integer.parseInt(conf.get("xRange"));
            yRange = Integer.parseInt(conf.get("yRange"));
            divisions = Integer.parseInt(conf.get("divisions"));
            dividerX = divisions;
            dividerY = divisions;
            subSpaceList = OutlierUtils.divideSampleSpaceBasedOnRadius(xRange, yRange, dividerX, dividerY);
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            double x = Double.parseDouble(values[0]);
            double y = Double.parseDouble(values[1]);
            List<String> subSpaceIdsForPoint = OutlierUtils.getListOfDivisionsForPoint(x, y, radius, subSpaceList);

            for (int i = 0; i < subSpaceIdsForPoint.size(); i++) {
                // key - subspace id
                // value - point
                context.write(new IntWritable(i), new Text(subSpaceIdsForPoint.get(i)));
            }
        }
    }

    public static class CustomReducer
            extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // input
            // key - subspace id
            // value - point
            String[] pointsInString;
            List<String> neglectList = new ArrayList<>();
            List<double[]> points = new ArrayList<>();
            for (Text point : values) {
                pointsInString = point.toString().split("_");
                double[] xy = {Double.parseDouble(pointsInString[0]), Double.parseDouble(pointsInString[1])};
                points.add(xy);
                if (point.toString().contains("N__orig"))
                    neglectList.add(xy[0] + "_" + xy[1]);
            }

            for (double[] point : points) {
                if (neglectList.contains(point[0] + "_" + point[1]))
                    continue;
                int count = OutlierUtils.getNumberOfPointsInCircle(point[0], point[1], radius, points);
                if (count < thresholdK)

                    // output
                    // key - "outlier"
                    // value - point
                    context.write(new Text(""), new Text(point[0] + "," + point[1]));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            throw new Exception("Pass all the required arguments. Input file, Output filepath, Output filename, radius, threshold");
        }
        Configuration conf = new Configuration();

        String inputDataPath = args[0];
        String outputPath = args[1];

        radius = Double.parseDouble(args[3]);
        thresholdK = Integer.parseInt(args[4]);
        conf.set("xRange", "10000");
        conf.set("yRange", "10000");
        conf.set("divisions", "100");
        conf.set("outputPath", outputPath);

        // add the below code if you are reading/writing from/to HDFS
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null) {
            throw new Exception("HADOOP_HOME not found. Please make sure system path has HADOOP_HOME point to hadoop installation directory");
        }
        conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);
        Job job = Job.getInstance(conf, "OutlierInXYSpace");

        job.setJarByClass(OutlierInXYSpace.class);
        job.setMapperClass(OutlierInXYSpace.CustomMapper.class);
        job.setReducerClass(OutlierInXYSpace.CustomReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        JSONInputFormat.addInputPath(job, new Path(inputDataPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        // 1. removing duplicates points which played a crucial role in counting the number of points around a given point
        //      but they are not required to be in output file
        // 2. Collecting all the reduver outputs and appending it to single file, ofcourse by removing duplicates

    }
}
