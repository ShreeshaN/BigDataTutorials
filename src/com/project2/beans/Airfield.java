package com.project2.beans;

import com.utils.StringConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Airfield implements Writable {

    private Text Id;
    private Text shortName;
    private Text name;
    private IntWritable flags;
    private IntWritable elevation;

    public Airfield(int flags, int elevation) {
        this.flags = new IntWritable(flags);
        this.elevation = new IntWritable(elevation);
    }

    public Airfield() {
        this.flags = new IntWritable();
        this.elevation = new IntWritable();
    }

    @Override
    public String toString() {
        return Id + StringConstants.COMMA + shortName + StringConstants.COMMA + name +
                StringConstants.COMMA + flags + StringConstants.COMMA + elevation;

    }

    public Text getId() {
        return Id;
    }

    public void setId(Text id) {
        Id = id;
    }

    public Text getShortName() {
        return shortName;
    }

    public void setShortName(Text shortName) {
        this.shortName = shortName;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public IntWritable getFlags() {
        return flags;
    }

    public void setFlags(IntWritable flags) {
        this.flags = flags;
    }

    public IntWritable getElevation() {
        return elevation;
    }

    public void setElevation(IntWritable elevation) {
        this.elevation = elevation;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        flags.write(dataOutput);
        elevation.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flags.readFields(dataInput);
        elevation.readFields(dataInput);
    }

}
