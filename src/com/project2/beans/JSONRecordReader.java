package com.project2.beans;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JSONRecordReader extends RecordReader<Text, Text> {
    LineRecordReader lineRecordReader;
    private Text key;
    private Text value;
    private long end;
    private boolean stillInChunk = true;
    private FSDataInputStream fsin;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private byte[] endTag = "}".getBytes();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//        this.fsplit = (FileSplit) inputSplit;
//        this.conf = taskAttemptContext.getConfiguration();
//        fs = fsplit.getPath().getFileSystem(conf);
//
//        fsinstream = fs.open(fsplit.getPath());
////        fsinstream.skip(fsplit.getStart());
//
//        splitStart = fsplit.getStart();
//        System.out.println("splitStart "+splitStart);
//        splitEnd = splitStart + fsplit.getLength();
//        System.out.println("splitEnd "+splitEnd);
//        br = new BufferedReader(new InputStreamReader(fsinstream));
//        String fullLine = "";
//        String skipline = "";
//        if ((skipline = br.readLine()) != null) {
//            fullLine += (skipline) + "_";
//        }

        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);

        fsin = fs.open(path);
        long start = split.getStart();
        end = split.getStart() + split.getLength();
        fsin.seek(start);

        if (start != 0) {
            readUntilMatch(endTag, false);
        }
//        lineRecordReader = new LineRecordReader();
//        lineRecordReader.initialize(inputSplit, taskAttemptContext);
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int b = fsin.read();
            if (b == -1)
                return false;
            if (withinBlock)
                buffer.write(b);
            if (b == match[i]) {
                i++;
                if (i >= match.length) {
                    return fsin.getPos() < end;
                }
            } else i = 0;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        StringBuilder s = new StringBuilder();
        if (!stillInChunk) return false;

        boolean status = readUntilMatch(endTag, true);
        value = new Text();
        String jsonString = new String(Arrays.copyOfRange(buffer.getData(), 0, buffer.getLength())).trim();

        // filters
        Predicate<String> hasFlags = e -> e.contains("Flags");
        Predicate<String> hasElevation = e -> e.contains("Elevation");
        String[] jsonvalues = jsonString.replace("}", "").replace("{", "").replace("\"", "").replace(": ", "_").trim().split(",");
        String requiredValues = Stream.of(jsonvalues).filter(hasFlags.or(hasElevation)).map(String::trim).collect(Collectors.joining(","));
        value.set(new Text(requiredValues));
        key = new Text(fsin.getPos() + "");
        buffer.reset();

        if (!status) {
            stillInChunk = false;
        }
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.0f;
    }

    @Override
    public void close() throws IOException {
        fsin.close();
    }

}
