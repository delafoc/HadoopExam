package com.mapreduce2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class map1 extends Mapper<Object, Text, Text, Text> {
    private Text text = new Text();

    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        text = value;
        context.write(text, new Text(""));
    }
}
