package com.project1.hadoop.queries;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query5 {
    /**
     *   Assume we want to design an analytics task on the data as follows:
     *     1) The Age attribute is divided into six groups, which are [10, 20), [20, 30), [30, 40), [40, 50), [50, 60), and [60, 70].
     *     The bracket “[“ means the lower bound of a range is included, where as “)” means the upper bound of a range is excluded.
     *     2) Within each of the above age ranges, further division is performed based on the “Gender”, i.e.,
     *        each of the 6 age groups is further divided into two groups.
     *     3) For each group, we need to report the following info:
     *     --> Age Range, Gender, MinTransTotal, MaxTransTotal, AvgTransTotal
     */


    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {

        List<String> customers = null;
        Map<String, String> customerIdAndAgeWithGenderMap = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            customers = new ArrayList<>();
            customerIdAndAgeWithGenderMap = new HashMap<>();
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                try {

                    String line = "";

                    // Create a FileSystem object and pass the
                    // configuration object in it. The FileSystem
                    // is an abstract base class for a fairly generic
                    // filesystem. All user code that may potentially
                    // use the Hadoop Distributed File System should
                    // be written to use a FileSystem object.
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());

                    // We open the file using FileSystem object,
                    // convert the input byte stream to character
                    // streams using InputStreamReader and wrap it
                    // in BufferedReader to make it more efficient
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                    while ((line = reader.readLine()) != null) {
                        customers.add(line);
                        String[] values = line.split(",");
                        String customerId = values[0];
                        String gender = values[3];
                        String age = values[2];
                        customerIdAndAgeWithGenderMap.putIfAbsent(customerId, age + "," + gender);
                    }
                } catch (Exception e) {
                    System.out.println("Unable to read customers cached File");
                    System.exit(1);
                }
            }
        }

        String getAge(String age) {
            if (age == null) {
                throw new NullPointerException("Passed age variable is null");
            }
            if (age.length() == 1) {
                return "0-10";
            }
            int start = Integer.parseInt(age.split("")[0]);
            int startPlusOne = start + 1;
            return start + "0-" + startPlusOne + "0";
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String transactionEntry = value.toString();
            String[] transactionValues = transactionEntry.split(",");
            String[] customerAgeAndGender = customerIdAndAgeWithGenderMap.get(transactionValues[1]).split(",");
            String ageRange = getAge(customerAgeAndGender[0]);
            String customerGender = customerAgeAndGender[1];
            // key - age range, gender
            // value - transaction total
            context.write(new Text(ageRange + "," + customerGender), new Text(transactionValues[2]));
        }
    }

    public static class CustomReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float minTransactiontotal = Integer.MAX_VALUE;
            float maxTransactionTotal = -1;
            float avgTransactionTotal = 0;
            float totalTransacionSum = 0;
            int count = 0;
            for (Text val : values) {
                count += 1;

                float transactionTotal = Float.parseFloat(val.toString());
                totalTransacionSum += transactionTotal;
                if (transactionTotal < minTransactiontotal) {
                    minTransactiontotal = transactionTotal;
                }
                if (transactionTotal > maxTransactionTotal) {
                    maxTransactionTotal = transactionTotal;
                }

            }
            avgTransactionTotal = totalTransacionSum / count;
            context.write(new Text("0"), new Text(key + "," + minTransactiontotal + "," + maxTransactionTotal + "," + avgTransactionTotal));
        }
    }


    public static void main(String[] args) throws Exception {
        String inputPathCustomers = "hdfs://localhost:9000/ds503/hw1/input/customers.txt";
        String inputPathTrasancations = "hdfs://localhost:9000/ds503/hw1/input/transactions.txt";
        String outputPath = "hdfs://localhost:9000/ds503/hw1/output/query5/";
        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));

        Job job = Job.getInstance(conf, "Query5");

        job.setJarByClass(Query5.class);
        job.setMapperClass(Query5.CustomMapper.class);
        job.setReducerClass(Query5.CustomReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);

        // Add distributed cache file
        try {
            job.addCacheFile(new URI(inputPathCustomers));
        } catch (Exception e) {
            System.out.println("Customers file Not Added");
            System.exit(1);
        }


        FileInputFormat.addInputPath(job, new Path(inputPathTrasancations));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
