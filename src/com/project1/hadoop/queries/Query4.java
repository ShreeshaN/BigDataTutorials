
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

public class Query4 {
    /**
     * Write a job(s) that reports for every country code, the number of customers having this
     * code as well as the min and max of TransTotal fields for the transactions done by those customers.
     * The output file should have one line for each country code containing:
     * --> CountryCode, NumberOfCustomers, MinTransTotal, MaxTransTotal
     * Hint: Do it in a single map-reduce job.
     */

    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {

        List<String> customers = null;
        Map<String, String> customerIdAndCountryCodeMap = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            customers = new ArrayList<>();
            customerIdAndCountryCodeMap = new HashMap<>();
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
                        String countryCode = values[4];
                        customerIdAndCountryCodeMap.putIfAbsent(customerId, countryCode);
                    }
                } catch (Exception e) {
                    System.out.println("Unable to read customers cached File");
                    System.exit(1);
                }
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String transactionStr = value.toString();
            String[] transactionValues = transactionStr.split(",");

            //key - country code
            //value - customer id + transtotal
            String customerCountryCode = customerIdAndCountryCodeMap.getOrDefault(transactionValues[1], "Country code not found");
            context.write(new Text(customerCountryCode), new Text(transactionValues[1] + "," + transactionValues[2]));
        }
    }

    public static class CustomReducer
            extends Reducer<Text, Text, Text, Text> {

        public int getTotalNumberCustomersForCountryCode() {
            return 0;
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //input
            //key - countrycode
            //value - list of <customer id + trans total>

            //output
            //key - countrycode
            //value - countrycode + total number of customers in that country code + min trans total + max trans total
            float minTransTotal = Integer.MAX_VALUE;
            float maxTransTotal = -1;
            Set<String> totalNumberOfCustomersinCountryCode = new HashSet<>();
            for (Text val : values) {
                String[] valueArr = val.toString().split(",");
                totalNumberOfCustomersinCountryCode.add(valueArr[0]);
                float transTotal = Float.parseFloat(valueArr[1]);
                if (transTotal < minTransTotal) {
                    minTransTotal = transTotal;
                }
                if (transTotal > maxTransTotal) {
                    maxTransTotal = transTotal;
                }
            }
            context.write(key, new Text(key + "," + totalNumberOfCustomersinCountryCode.size() + "," + minTransTotal + "," + maxTransTotal));
        }
    }


    public static void main(String[] args) throws Exception {
        String inputPathTransactions = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/transactions.txt";
        String inputPathCustomers = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/customers.txt";
        String outputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/shree_output/query4/";

        Configuration conf = new Configuration();

        // add the below code if you are reading/writing from/to HDFS
        // String inputPathTransactions = "hdfs://localhost:9000/ds503/hw1/input/transactions.txt";
        // String inputPathCustomers = "hdfs://localhost:9000/ds503/hw1/input/customers.txt";
        // String outputPath = "hdfs://localhost:9000/ds503/hw1/output/query4/";
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));

        Job job = Job.getInstance(conf, "Query4SingleMapReduceUsingDistributedCache");

        job.setJarByClass(Query4.class);
        job.setMapperClass(Query4.CustomMapper.class);
        job.setReducerClass(Query4.CustomReducer.class);

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


        FileInputFormat.addInputPath(job, new Path(inputPathTransactions));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
