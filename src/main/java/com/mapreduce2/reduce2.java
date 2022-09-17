package com.mapreduce2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class reduce2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static IntWritable num = new IntWritable(1);
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            context.write(num, key);
            num = new IntWritable(num.get() + 1);
        }
    }
}
