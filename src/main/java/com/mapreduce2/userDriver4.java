package com.mapreduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class userDriver4 {
    static class allMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String city = value.toString();
            context.write(new Text(city), new IntWritable(1));
        }
    }

    static class someMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split(" ");
            context.write(new Text(lines[0]), new IntWritable(1));
        }
    }

    static class reduce4 extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable value : values) {
                num++;
            }
            if (num == 1){
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

        String input1 = "/input/4duobiao/allCity.txt";
        String input2 = "/input/4duobiao/someCity.txt";
        String output = "/work/test6";

        //设置job名称
        job.setJobName("Job4_duobiao");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(userDriver4.class);

        //map函数位置
//        job.setMapperClass(allMap.class);
//        job.setCombinerClass(reduce3.class);
//        job.setReducerClass(reduce4.class);

        MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, allMap.class);
        MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, someMap.class);
        job.setReducerClass(reduce4.class);

        //map设置出来的
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce设置出来的
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //删除已经存在的output文件夹
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
        Path outputPath = new Path(output);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        //指定本次job待处理数据的目录和程序执行完输出结果存放的目录
        long startTime = System.currentTimeMillis(); //获取当前时间

//        FileInputFormat.addInputPath(job, new Path(input1));
//        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        System.exit(result ? 0 : -1);
    }
}
