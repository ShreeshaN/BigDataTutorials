/**
 * @created on: 18/3/19,
 * @author: Shreesha N,
 * @version: v0.0.1
 * @system name: badgod
 * Description:
 * <p>
 * ..todo::
 */

package com.project1.hadoop.queries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Query3 {
    /**
     * Write a job(s) that joins the Customers and Transactions datasets
     * (based on the customer ID) and reports for each customer the following info:
     * --> CustomerID, Name, Salary, NumOf Transactions, TotalSum, MinItems
     * Where NumOfTransactions is the total number of transactions done by the customer,
     * TotalSum is the sum of field “TransTotal” for that customer,
     * and MinItems is the minimum number of items in transactions done by the customer.
     */

    public static class CustomerMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String customerEntry = value.toString();
            String[] customerValues = customerEntry.split(",");
            // key - customer id
            // value - customer name, salary
            context.write(new Text(customerValues[0]), new Text("customer," + customerValues[1] + "," + customerValues[5]));
        }
    }

    public static class TransactionMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String transactionEntry = value.toString();
            String[] transactionValues = transactionEntry.split(",");

            // key - customer id
            // value - transaction num items, transaction total
            context.write(new Text(transactionValues[1]), new Text("transaction," + transactionValues[3] + "," + transactionValues[2]));
        }
    }

    public static class CustomJoinReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String customerName = null;
            String customerSalary = null;
            int minTransactions = Integer.MAX_VALUE;
            int totalNumberOfTransactions = 0;
            float totalTransactionSum = 0;
            for (Text val : values) {
                String valueString = val.toString();
                String[] valueArr = valueString.split(",");
                if (valueString.contains("customer")) {
                    customerName = valueArr[1];
                    customerSalary = valueArr[2];
                } else {
                    if (Integer.parseInt(valueArr[1]) < minTransactions) {
                        minTransactions = Integer.parseInt(valueArr[1]);
                    }
                    totalTransactionSum += Float.parseFloat(valueArr[2]);
                    totalNumberOfTransactions += 1;
                }
            }
            // key - customer id
            // value - customer id, customer name, salary, total number of transactions, total transaction sum, min transaction items
            context.write(key, new Text(key + "," + customerName + "," + customerSalary + "," + totalNumberOfTransactions + "," + totalTransactionSum + "," + minTransactions));
        }
    }


    public static void main(String[] args) throws Exception {

        String inputPathCustomers = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/customers.txt";
        String inputPathTrasancations = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/transactions.txt";
        String outputPath = "/Users/badgod/badgod_documents/github/BigDataTutorials/output/shree_output/query3";


        Configuration conf = new Configuration();
        // add the below code if you are reading/writing from/to HDFS
        // String inputPathCustomers = "hdfs://localhost:9000/ds503/hw1/input/customers.txt";
        // String inputPathTrasancations = "hdfs://localhost:9000/ds503/hw1/input/transactions.txt";
        // String outputPath = "hdfs://localhost:9000/ds503/hw1/output/query3/";
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/core-site.xml"));
        // conf.addResource(new Path("/Users/badgod/badgod_documents/technologies/hadoop-3.1.2/etc/hadoop/hdfs-site.xml"));

        Job job = Job.getInstance(conf, "Query3JoinReducer");

        job.setJarByClass(Query3.class);
        job.setMapperClass(Query3.CustomerMapper.class);
        job.setMapperClass(Query3.TransactionMapper.class);
        job.setReducerClass(Query3.CustomJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);

        MultipleInputs.addInputPath(job, new Path(inputPathCustomers), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPathTrasancations), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
