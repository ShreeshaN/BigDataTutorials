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
import com.utils.GeneralUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


// doubts
// 1. if i load cache, will it be distributed to all mappers ? So in that case it is ok to maintain global variables
//          and set them in setup of mapper ?
// 2. if i have 4 nodes, how to merge the output written by reducer into one single file ?
//          (outputformatter will just format output on node level,right ?)


public class OutlierInXYSpace {


    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {
        private int xRange;
        private int yRange;
        private int radius;
        private int thresholdK;
        private int numberOfSubspacesInXYPlane;
        private int dividerX;
        private int dividerY;
        List<SubSpace> subSpaceList;
        int divisions;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            xRange = Integer.parseInt(conf.get("xRange"));
            yRange = Integer.parseInt(conf.get("yRange"));
            radius = Integer.parseInt(conf.get("radius"));
            thresholdK = Integer.parseInt(conf.get("thresholdK"));
            divisions = Integer.parseInt(conf.get("divisions"));
            dividerX = divisions;
            dividerY = divisions;
            subSpaceList = OutlierUtils.divideSampleSpaceBasedOnRadius(xRange, yRange, dividerX, dividerY);
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            int x = Integer.parseInt(values[0]);
            int y = Integer.parseInt(values[1]);
            Map<String, String> subSpaceIdsForPoint = OutlierUtils.getListOfDivisionsForPoint(x, y, radius, subSpaceList);


            for (String subspaceId : subSpaceIdsForPoint.keySet()) {
                // key - subspace id
                // value - point
                context.write(new Text(subspaceId), new Text(subSpaceIdsForPoint.get(subspaceId)));
            }
        }
    }

    public static class CustomReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // input
            // key - subspace id
            // value - point
            if (!(key.toString().contains("N_orig"))) {
                Configuration conf = context.getConfiguration();
                String[] pointsInString;
                int radius = Integer.parseInt(conf.get("radius"));
                int thresholdK = Integer.parseInt(conf.get("thresholdK"));
                List<String> neglectList = new ArrayList<>();
                List<int[]> points = new ArrayList<>();
                for (Text point : values) {
                    pointsInString = point.toString().split("_");
                    int[] xy = {Integer.parseInt(pointsInString[0]), Integer.parseInt(pointsInString[1])};
                    points.add(xy);
                    if (point.toString().contains("N__orig"))
                        neglectList.add(xy[0] + "_" + xy[1]);
                }

                for (int[] point : points) {
                    if (neglectList.contains(point[0] + "_" + point[1]))
                        continue;
                    int count = OutlierUtils.getNumberOfPointsInCircle(point[0], point[1], radius, points);
                    if (count < thresholdK)

                        // output
                        // key - "outlier"
                        // value - point
                        context.write(new Text("Outlier"), new Text(point[0] + "," + point[1]));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String inputDataPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/xy_coordinates_test2";
        String outputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/project2/outlier_xy_space_test2/";

        conf.set("xRange", "20");
        conf.set("yRange", "20");
        conf.set("radius", "4");
        conf.set("thresholdK", "4");
        conf.set("divisions", "50");
        conf.set("outputPath", outputPath);


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

        int reducers = job.getNumReduceTasks();

        // removing duplicates which help in counting the number of points around a given point
        // but they are not required to be in output file
        for (int i = 0; i < reducers; i++) {
            GeneralUtilities.removeDuplicates(outputPath + "/part-r-0000" + i, FileSystem.get(conf));
        }


    }
}
