package com.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class myDriver {
        public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();

//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("fs.defaultFS", "hdfs://master:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        String input = "/input/sogou.500w.utf8";
        String output = "/test";

        Job job = Job.getInstance(conf);

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");


        //主函数位置
        job.setJarByClass(myDriver.class);
        //map函数位置
        job.setMapperClass(myMapper3.class);
        job.setReducerClass(myReduce3.class);
        //map设置出来的
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce设置出来的
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //删除已经存在的output文件夹
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
        Path outputPath = new Path(output);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

//        job.setJobName("mapreduce1");
//        job.setJobName("mapreduce2");
        job.setJobName("mapreduce3");
        //指定本次job待处理数据的目录和程序执行完输出结果存放的目录
        long startTime = System.currentTimeMillis(); //获取当前时间

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        System.exit(result ? 0 : -1);
    }
//    public static void main(String[] args) throws Exception {
//
//        //hadoop用户不然不能拥有写进去的权限
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
//
//        //hdfs的地址
//        Configuration configuration = new Configuration();
//        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        configuration.set("mapreduce.app-submission.cross-platform", "true");
////        configuration.set("dfs.client.use.datanode.hostname", "true");
//
//        //输入输出的文件m目录
//        String input = "/input/sogou.500w.utf8";
//        String output = "/output3";
//
//        //FileUtil.deleteDir(output);
//        Job job = Job.getInstance(configuration);
//        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");
//
//
//        //主函数位置
//        job.setJarByClass(myDriver.class);
//        //map函数位置
//        job.setMapperClass(myMapper3.class);
//        job.setReducerClass(myReduce3.class);
//        //map设置出来的
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        //reduce设置出来的
//        job.setOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        //删除已经存在的output文件夹
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), configuration, "hadoop");
//        Path outputPath = new Path(output);
//        if (fileSystem.exists(outputPath)) {
//            fileSystem.delete(outputPath, true);
//        }
//
////        job.setJobName("mapreduce1");
////        job.setJobName("mapreduce2");
//        job.setJobName("mapreduce3");
//        //指定本次job待处理数据的目录和程序执行完输出结果存放的目录
//        long startTime = System.currentTimeMillis(); //获取当前时间
//
//        FileInputFormat.setInputPaths(job, new Path(input));
//        FileOutputFormat.setOutputPath(job, outputPath);
//
//        //提交job
//        boolean result = job.waitForCompletion(true);
//        long endTime = System.currentTimeMillis(); //获取结束时间
//        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
//        System.exit(result ? 0 : -1);
//
//    }
}
