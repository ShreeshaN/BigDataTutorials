package com.project2.custominputformat;

import com.project2.beans.JSONRecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class JSONInputFormat extends FileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        JSONRecordReader jsonRecordReader = new JSONRecordReader();
        jsonRecordReader.initialize(inputSplit, taskAttemptContext);
        return jsonRecordReader;
    }
}
