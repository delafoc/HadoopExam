package com.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myReduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    static int sum = 0;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int y = 0;
        for (IntWritable value : values) {
            y += value.get();
        }
        sum += y;
        context.write(new Text(key + "\t此Uid数量为：" + y + "\t累计数量为："), new IntWritable(sum));
    }
}
