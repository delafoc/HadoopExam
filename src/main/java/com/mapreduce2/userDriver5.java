package com.mapreduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class userDriver5 {
    static class map5 extends Mapper<Object, Text, StudentWritable, NullWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, StudentWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split(" ");
            if (!lines[0].equals("Math")){
//                System.out.println(lines[0]);
//                System.out.println(lines[1]);
                StudentWritable studentWritable = new StudentWritable(Long.parseLong(lines[0]), Long.parseLong(lines[1]));
                context.write(studentWritable, NullWritable.get());
            }
        }
    }

    static class reduce5 extends Reducer<StudentWritable, NullWritable, StudentWritable, Text> {

        @Override
        protected void reduce(StudentWritable key, Iterable<NullWritable> values, Reducer<StudentWritable, NullWritable, StudentWritable, Text>.Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key, new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

        String input1 = "/input/5ercipaixu/chengji.txt";
        String output = "/work/test7";

        //设置job名称
        job.setJobName("Job5_ercipaixu");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(userDriver5.class);

        //map函数位置
        job.setMapperClass(map5.class);
        job.setReducerClass(reduce5.class);

        //map设置出来的
        job.setMapOutputKeyClass(StudentWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //reduce设置出来的
        job.setOutputKeyClass(StudentWritable.class);
        job.setOutputValueClass(Text.class);

        //删除已经存在的output文件夹
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
        Path outputPath = new Path(output);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        //指定本次job待处理数据的目录和程序执行完输出结果存放的目录
        long startTime = System.currentTimeMillis(); //获取当前时间

        FileInputFormat.addInputPath(job, new Path(input1));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        System.exit(result ? 0 : -1);
    }
}
