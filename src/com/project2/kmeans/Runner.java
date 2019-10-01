package com.project2.kmeans;

import com.utils.GeneralUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class Runner {


    public static boolean compareCentroids(String oldCentroidFile, String newCentroidFile, FileSystem fs) throws IOException {
        List<String> oldCentroids = GeneralUtilities.readFileIntoIterableHDFS(oldCentroidFile, fs);
        oldCentroids.replaceAll(String::trim);
        List<String> newCentroids = GeneralUtilities.readFileIntoIterableHDFS(newCentroidFile, fs);
        newCentroids.replaceAll(String::trim);

        Collections.sort(oldCentroids);
        Collections.sort(newCentroids);
        return oldCentroids.equals(newCentroids);
    }

    public static void main(String[] args) throws Exception {
        // String inputPathCustomers = "hdfs://localhost:9000//ds503/hw2/input/centroids.txt";
        // String outputPath = "hdfs://localhost:9000/ds503/hw1/output/query2/"

//        String inputRectangles = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/rectangles.txt";
//        String oldCentroidsPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/centroids.txt";
//        String newCentroidsPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/project2/kmeans/";
        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        String inputRectangles = "hdfs://localhost:9000/ds503/hw2/input/rectangles.txt";
        String oldCentroidsPath = "hdfs://localhost:9000/ds503/hw2/input/centroids.txt";
        String newCentroidsPath = "hdfs://localhost:9000/ds503/hw2/output/kmeans";
        String[] arguments = {inputRectangles, oldCentroidsPath, newCentroidsPath};
        for (int i = 0; i < 6; i++) {
            System.out.println("Running kmeans - " + i);
            KMeans.main(arguments);
            if (Runner.compareCentroids(oldCentroidsPath, newCentroidsPath + "part-r-00000", fs)) {
                System.out.println("Kmeans algorithm converged at " + i + 1 + " iterations. Terminating");
                break;
            }
            List<String> newCentroids = GeneralUtilities.readFileIntoIterableHDFS(newCentroidsPath + "part-r-00000", fs);
            GeneralUtilities.writeIterableToFileHDFS(newCentroids, oldCentroidsPath, fs);
        }
//
    }
}

