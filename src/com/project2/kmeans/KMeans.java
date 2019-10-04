/**
 * @created on: 18/3/19,
 * @author: Shreesha N,
 * @version: v0.0.1
 * @system name: badgod
 * Description:
 * <p>
 * ..todo::
 */

package com.project2.kmeans;

import com.utils.GeneralUtilities;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


public class KMeans {
    /**
     * Refer to project2.pdf - problem 2 for question.
     */

    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {

        List<List<Float>> centroids = null;
        Map<String, Integer> centroidIdMap = new HashMap<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            centroids = new ArrayList<>();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {

                    String line = "";
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                    int lineNumber = 0;
                    while ((line = reader.readLine()) != null) {
                        lineNumber++;
                        String[] values = line.split(",");
                        List<Float> c = new ArrayList<>();
                        for (int i = 0; i < values.length; i++) {
                            c.add(Float.parseFloat(values[i]));
                        }
                        centroidIdMap.putIfAbsent(StringUtils.join(c, ","), lineNumber);
                        centroids.add(c);

                    }
                } catch (Exception e) {
                    System.out.println("Unable to read centroids cached File");
                    System.exit(1);
                }
            }
        }

        public float getEuclideanDistance(List<Float> arr1, List<Float> arr2) {
            float sum = 0;
            for (int i = 0; i < arr1.size(); i++) {
                sum = sum + (arr1.get(i) - arr2.get(i)) * (arr1.get(i) - arr2.get(i));
            }
            return (float) Math.sqrt(sum);
        }

        public List<Float> getClosestCentroidForDataPoint(List<List<Float>> centroids, List<Float> dataPoint) {
            float leastDistance = Integer.MAX_VALUE;
            List<Float> closestCentroid = new ArrayList<>();
            for (int i = 0; i < centroids.size(); i++) {
                float distance = getEuclideanDistance(centroids.get(i), dataPoint);
                if (distance < leastDistance) {
                    closestCentroid = centroids.get(i);
                    leastDistance = distance;
                }
            }
            return closestCentroid;

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            List<Float> dataPointList = new ArrayList<>();
            Float[] dataPoint = Arrays.stream(value.toString().split(",")).map(Float::valueOf).toArray(Float[]::new);
            Collections.addAll(dataPointList, dataPoint);
            List<Float> closestCentroid = getClosestCentroidForDataPoint(centroids, dataPointList);

            String k = centroidIdMap.get(StringUtils.join(closestCentroid, ",")).toString();
            // key - id of centroid that matches the data point
            // value - data point
            context.write(new Text(k), new Text(value));
        }
    }

    public static class CustomCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // input
            // key - centroid id
            // value - list of data points belonging to the centroid

            List<Float> summedUpValues = new ArrayList<>();
            int count = 0;
            for (Text val : values) {
                String[] dataPoint = val.toString().split(",");
                for (int j = 0; j < dataPoint.length; j++) {
                    if (count == 0)
                        summedUpValues.add(Float.parseFloat(dataPoint[j]));
                    else
                        summedUpValues.set(j, summedUpValues.get(j) + Float.parseFloat(dataPoint[j]));
                }
                count++;
            }
            // output
            // key - centroid id
            // value - summed up values as one string, count
            context.write(key, new Text(StringUtils.join(summedUpValues, ",") + "," + count));
        }
    }

    public static class CustomReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // input
            // key - centroid id
            // value - summed up values as one string, count
            int globalCount = 0;
            int reducerCount = 0;
            List<Float> summedUpValues = new ArrayList<>();
            for (Text val : values) {
                String[] dataPoint = val.toString().split(",");
                globalCount += Integer.parseInt(dataPoint[dataPoint.length - 1]);
                for (int j = 0; j < dataPoint.length - 1; j++) {
                    if (reducerCount == 0)
                        summedUpValues.add(Float.parseFloat(dataPoint[j]));
                    else
                        summedUpValues.set(j, summedUpValues.get(j) + Float.parseFloat(dataPoint[j]));
                }
                reducerCount++;
            }
            // finding the new average i.e the centroid
            for (int i = 0; i < summedUpValues.size(); i++) {
                summedUpValues.set(i, summedUpValues.get(i) / globalCount);
            }
            // key - centroid id
            // value - new centroid
            context.write(new Text(""), new Text(StringUtils.join(summedUpValues, ",")));
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputData = "hdfs://localhost:9000/ds503/hw2/input/p_sample.txt";
        String inputCentroidsPath = "hdfs://localhost:9000/ds503/hw2/input/sample_centroids.txt";
        String newCentroidsPath = "hdfs://localhost:9000/ds503/hw2/output¬/kmeans/";
        String centroidsFilename = "centroids.txt";
        int numOfReducers = 5;

        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(newCentroidsPath), true);
        for (int i = 0; i < 6; i++) {
            int iMinus1 = i - 1;
            System.out.println("Running kmeans for " + i);
            Job job = Job.getInstance(conf, "KMeans");

            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeans.CustomMapper.class);
            job.setCombinerClass(KMeans.CustomCombiner.class);
            job.setReducerClass(KMeans.CustomReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputData));
            FileOutputFormat.setOutputPath(job, new Path(newCentroidsPath + "/" + i));

            // Add distributed cache file
            try {
                if (i == 0)
                    job.addCacheFile(new URI(inputCentroidsPath));
                else
                    job.addCacheFile(new URI(newCentroidsPath + "/" + iMinus1 + "/" + centroidsFilename));

            } catch (Exception e) {
                System.out.println("Centroids file Not Added");
                System.exit(1);
            }
            job.setNumReduceTasks(numOfReducers);
            job.waitForCompletion(true);
            List<String> intermediateCentroids = new ArrayList<>();

            // iterate over all the reducer outputs and merge them together, write it to a single file
            for (int j = 0; j < numOfReducers; j++) {
                intermediateCentroids.addAll(GeneralUtilities.readFileIntoIterableHDFS(newCentroidsPath + "/" + i + "/part-r-0000" + j, fs));
            }
            GeneralUtilities.writeIterableToFileHDFS(intermediateCentroids, newCentroidsPath + "/" + i + "/" + centroidsFilename, fs);


            // if i==0 which means its the first iteration, do not run the compare logic
            if (i != 0) {

                // compare logic, pick the single file that was saved in previous step and compare it to the single file that was
                List<String> oldCentroids = GeneralUtilities.readFileIntoIterableHDFS(newCentroidsPath + iMinus1 + "/" + centroidsFilename, fs);
                List<String> newCentroids = GeneralUtilities.readFileIntoIterableHDFS(newCentroidsPath + i + "/" + centroidsFilename, fs);
                if (GeneralUtilities.compareLists(oldCentroids, newCentroids)) {
                    System.out.println("Kmeans algorithm converged at " + i + " iterations. Terminating");
                    break;
                }
            }
        }
        fs.close();
    }

}
