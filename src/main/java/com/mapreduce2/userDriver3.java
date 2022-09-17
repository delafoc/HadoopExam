package com.mapreduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class userDriver3 {
    static class map3 extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split(" ");
            context.write(new Text(lines[0]), new IntWritable(Integer.parseInt(lines[1])));
        }
    }

    static class reduce3 extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            int num = 0;
            double aver = 0;
            for (IntWritable value : values) {
                aver += value.get();
                num++;
            }
            aver = aver/num;
            context.write(key, new DoubleWritable(aver));
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

        String input1 = "/input/3pinjun/chinese.txt";
        String input2 = "/input/3pinjun/English.txt";
        String input3 = "/input/3pinjun/math.txt";
        String output = "/work/test5";

        //设置job名称
        job.setJobName("Job3_pinjun");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(userDriver3.class);

        //map函数位置
        job.setMapperClass(map3.class);
//        job.setCombinerClass(reduce3.class);
        job.setReducerClass(reduce3.class);

        //map设置出来的
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce设置出来的
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //删除已经存在的output文件夹
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
        Path outputPath = new Path(output);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        //指定本次job待处理数据的目录和程序执行完输出结果存放的目录
        long startTime = System.currentTimeMillis(); //获取当前时间

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileInputFormat.addInputPath(job, new Path(input3));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        System.exit(result ? 0 : -1);
    }
}
