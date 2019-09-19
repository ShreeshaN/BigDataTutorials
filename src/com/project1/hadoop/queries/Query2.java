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

public class Query2 {

    /**
     * Write a job(s) that reports for every customer, the number of transactions that customer did and the total sum of these transactions.
     * The output file should have one line for each customer containing:
     * --> CustomerID, CustomerName, NumTransactions, TotalSum
     * You are required to use a Combiner in this query.
     */

    public static class CustomMapper
            extends Mapper<Object, Text, Text, Text> {

        List<String> customers = null;
        Map<String, String> customerIdAndNameMap = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            customers = new ArrayList<>();
            customerIdAndNameMap = new HashMap<>();
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
                        String customerName = values[1];
                        customerIdAndNameMap.putIfAbsent(customerId, customerName);
                    }
                } catch (Exception e) {
                    System.out.println("Unable to read customers cached File");
                    System.exit(1);
                }
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] transactionValues = value.toString().split(",");

            // key - customer id
            // value - customer name + transaction total
            // Alternative - here customer id and customer name can be concatenated and sent as key
            context.write(new Text(transactionValues[1]), new Text(customerIdAndNameMap.get(transactionValues[1]) + "," + transactionValues[2]));
        }
    }

    public static class CustomCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {


            int totalTransactionsPerCustomer = 0;
            double transactionTotalPerCustomer = 0.0;
            String customerName = null;
            for (Text val : values) {
                String[] transValues = val.toString().split(",");
                totalTransactionsPerCustomer += 1;
                transactionTotalPerCustomer += Double.parseDouble(transValues[1]);
                customerName = transValues[0];
            }

            // key - customer id
            // value - customer name + total number of transactions + transaction total
            context.write(key, new Text(customerName + "," + Integer.toString(totalTransactionsPerCustomer).trim() + "," + Double.toString(transactionTotalPerCustomer).trim()));
        }
    }

    public static class CustomReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {


            int totalTransactionsPerCustomer = 0;
            double transactionTotalPerCustomer = 0.0;
            String customerName = null;
            for (Text val : values) {
                String[] transValues = val.toString().split(",");
                totalTransactionsPerCustomer += Integer.parseInt(transValues[1]);
                transactionTotalPerCustomer += Double.parseDouble(transValues[2]);
                customerName = transValues[0];
            }
            // key - customer id
            // value - customer name + total number of transactions + transaction total
            context.write(key, new Text(key + "," + customerName + "," + Integer.toString(totalTransactionsPerCustomer).trim() + "," + Double.toString(transactionTotalPerCustomer).trim()));
        }
    }


    public static void main(String[] args) throws Exception {
        String inputPathTransactions = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/transactions.txt";
        String inputPathCustomers = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/customers.txt";
        String outputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/shree_output/query2/";

        Configuration conf = new Configuration();

        // add the below code if you are reading/writing from/to HDFS
        // String inputPathTransactions = "hdfs://localhost:9000/ds503/hw1/input/transactions.txt";
        // String inputPathCustomers = "hdfs://localhost:9000/ds503/hw1/input/customers.txt";
        // String outputPath = "hdfs://localhost:9000/ds503/hw1/output/query2/";
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));

        Job job = Job.getInstance(conf, "Query2UsingDistributedCache");

        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2.CustomMapper.class);
        job.setCombinerClass(Query2.CustomCombiner.class);
        job.setReducerClass(Query2.CustomReducer.class);

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
