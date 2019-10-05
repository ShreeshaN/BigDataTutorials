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
import java.util.ArrayList;
import java.util.List;


public class KMeans {
    /**
     * Refer to project2.pdf - problem 2 for question.
     */

    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {

        List<List<Double>> centroids = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            centroids = new ArrayList<>();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {

                    String line = "";
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                    while ((line = reader.readLine()) != null) {
                        String[] values = line.split(",");
                        List<Double> c = new ArrayList<>();
                        for (int i = 0; i < values.length; i++) {
                            c.add(Double.parseDouble(values[i].trim()));
                        }
                        centroids.add(c);

                    }
                } catch (Exception e) {
                    System.out.println("Unable to read centroids cached File");
                    System.exit(1);
                }
            }
        }

        public double getEuclideanDistance(List<Double> arr1, List<Double> arr2) {
            double sum = 0;
            for (int i = 0; i < arr1.size(); i++) {
                sum = sum + (arr1.get(i) - arr2.get(i)) * (arr1.get(i) - arr2.get(i));
            }
            return Math.sqrt(sum);
        }

        public List<Double> getClosestCentroidForDataPoint(List<List<Double>> centroids, List<Double> dataPoint) {
            double leastDistance = Integer.MAX_VALUE;
            List<Double> closestCentroid = new ArrayList<>();
            for (int i = 0; i < centroids.size(); i++) {
                double distance = getEuclideanDistance(centroids.get(i), dataPoint);
                if (distance < leastDistance) {
                    closestCentroid = centroids.get(i);
                    leastDistance = distance;
                }
            }
            return closestCentroid;

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            List<Double> dataPointList = new ArrayList<>();
            String[] dataPointStr = value.toString().split(",");
            for (String s : dataPointStr)
                dataPointList.add(Double.parseDouble(s));
            List<Double> closestCentroid = getClosestCentroidForDataPoint(centroids, dataPointList);

            // key - id of centroid that matches the data point
            // value - data point
            String a = StringUtils.join(dataPointList, ",");
            if (a.split(",").length > 2) {
                System.out.println("");
            }
            context.write(new Text(StringUtils.join(closestCentroid, ",")), new Text(StringUtils.join(dataPointList, ",")));
        }
    }

    public static class CustomCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // input
            // key - centroid
            // value - list of data points belonging to the centroid
            List<Integer> summedUpValues = new ArrayList<>();
            int count = 0;
            for (Text val : values) {
                String[] dataPoint = val.toString().split(",");
                for (int i = 0; i < dataPoint.length; i++) {
                    if (count == 0) {
                        summedUpValues.add((int) Double.parseDouble(dataPoint[i]));
                    } else {
                        summedUpValues.set(i, (int) ((summedUpValues.get(i) + Double.parseDouble(dataPoint[i]))));
                    }
                }
                count++;

            }

            // output
            // key - centroid
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
            // key - centroid
            // value - summed up values as one string, count
            int localCount = 0;
            int globalCount = 0;
            List<Double> summedUpValues = new ArrayList<>();
            double[] arr = {0, 0};
            for (Text val : values) {
                String[] dataPoint = val.toString().split(",");
                globalCount += Double.parseDouble(dataPoint[dataPoint.length - 1]);
                for (int i = 0; i < dataPoint.length - 1; i++) {
                    if (localCount == 0) {
                        summedUpValues.add(Double.parseDouble(dataPoint[i]));
                    } else {
                        summedUpValues.set(i, summedUpValues.get(i) + Double.parseDouble(dataPoint[i]));
                    }
                }
                localCount++;

            }
            // finding the new average i.e the centroid
            for (int i = 0; i < summedUpValues.size(); i++) {
                summedUpValues.set(i, summedUpValues.get(i) / globalCount);
            }

            // key - empty
            // value - new centroid
            context.write(new Text(""), new Text(arr[0] + "," + arr[1]));
        }

    }


    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            throw new Exception("Pass all the required arguments. Input file, Centroids file path, Output filepath, number of iterations");
        }
        Configuration conf = new Configuration();
        String inputData = args[0];
        String inputCentroidsPath = args[1];
        String newCentroidsPath = args[2];

        String centroidsFilename = "centroids.txt";
        int numOfReducers = 1;
        int numOfIterations = Integer.parseInt(args[3]);
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null) {
            throw new Exception("HADOOP_HOME not found. Please make sure system path has HADOOP_HOME point to hadoop installation directory");
        }
        conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(newCentroidsPath), true);
        for (int i = 0; i < numOfIterations; i++) {
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
                throw new Exception("Centroids file Not Added");
            }
            job.setNumReduceTasks(numOfReducers);
            job.waitForCompletion(true);
            List<String> intermediateCentroids = new ArrayList<>();

            // iterate over all the reducer outputs and merge them together, write it to a single file
            for (int j = 0; j < numOfReducers; j++) {
                intermediateCentroids.addAll(GeneralUtilities.readFileIntoIterableHDFS(newCentroidsPath + "/" + i + "/part-r-0000" + j, fs));
            }
            GeneralUtilities.writeIterableToFileHDFS(intermediateCentroids, newCentroidsPath + "/" + i + "/" + centroidsFilename, fs);


//             if i==0 which means its the first iteration, do not run the compare logic
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
    }

}
