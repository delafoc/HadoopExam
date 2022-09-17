package com.shixun_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;

class map5 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().trim().split(",");
        if (strings[3].length() > 0 && strings[5].length() > 0)
            context.write(new Text(strings[3]), new Text(strings[5]));
    }
}

class reduce5 extends Reducer<Text, Text, Text, Text> {
//    HashMap<String, Long> yeezyMap = new HashMap<String, Long>(); // yeezy
//    HashMap<String, Long> off_whiteMap = new HashMap<String, Long>(); // off_white

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("运动鞋名称" + "\t" + "最低与最高销售价"), new Text(""));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        ArrayList<String> arrayList = new ArrayList<>();
        for (Text value : values) {
            arrayList.add(value.toString());
        }
        arrayList.sort(Comparator.naturalOrder());
        LinkedHashSet<String> hashSet = new LinkedHashSet<>(arrayList);
        ArrayList<String> listWithoutDuplicates = new ArrayList<>(hashSet);
        String s = listWithoutDuplicates.get(0);
        String s1 = listWithoutDuplicates.get(listWithoutDuplicates.size() - 1);
        context.write(new Text(key.toString()), new Text(s + "-" + s1));
//
//        for (Text value : values) {
//            context.write(new Text(key.toString()), new Text(value));
//        }
    }
}

public class shixun5 {
    //    5.每种品牌类别最高与最低零售价数据分析统计
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

        String input = "/shixun/datas/newSneaker_data.csv";
        String output = "/shixun/output5";

        //设置job名称
        job.setJobName("Job3_shixun5");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(shixun5.class);

        //map函数位置
        job.setMapperClass(map5.class);
//        job.setCombinerClass(reduce.class);
        job.setReducerClass(reduce5.class);

        //map设置出来的
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

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

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        System.exit(result ? 0 : -1);
    }
}
