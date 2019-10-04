/**
@created on: 18/3/19,
@author: Shreesha N,
@version: v0.0.1
@system name: badgod
Description:

..todo::

*/

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
    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
        // the size of airfield.json is 967618 bytes. So to create 5 splits we need to divide the file by 200000
        return 200000;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        JSONRecordReader jsonRecordReader = new JSONRecordReader();
        jsonRecordReader.initialize(inputSplit, taskAttemptContext);
        return jsonRecordReader;
    }
}
