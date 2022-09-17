package com.mapreduce2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class map2 extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable intWritable = new IntWritable(1);
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        IntWritable line = new IntWritable(Integer.parseInt(value.toString()));
        context.write(line, intWritable);
    }
}
