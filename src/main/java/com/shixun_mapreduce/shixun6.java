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
import java.util.*;

class map6 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().trim().split(",");
        if (strings[3].length() > 0 && strings[5].length() > 0)
            context.write(new Text(strings[3]), new Text("1"));
    }
}

class reduce6 extends Reducer<Text, Text, Text, Text> {
        HashMap<Long, String> yeezyMap = new HashMap<Long, String>(); // yeezy
        HashMap<Long, String> off_whiteMap = new HashMap<Long, String>(); // off_white
//    ArrayList<String> yerzyList = new ArrayList<>();
//    ArrayList<String> off_whiteList = new ArrayList<>();


    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("按销售量排序" + "\t" + "品牌"), new Text(""));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        if (key.toString().contains("Yeezy")){
            long num = 0;
            for (Text value : values) {
                num++;
            }
            yeezyMap.put(num, key.toString());
        } else {
            long num = 0;
            for (Text value : values) {
                num++;
            }
            off_whiteMap.put(num, key.toString());
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        TreeMap<Long, String> treeMap1 = new TreeMap<>(yeezyMap);
        TreeMap<Long, String> treeMap2 = new TreeMap<>(off_whiteMap);
        context.write(new Text("Yeezy: "), new Text(""));
        Set<Long> longs = treeMap1.keySet();
        for (Long aLong : longs) {
            context.write(new Text(aLong + "\t" + treeMap1.get(aLong)), new Text(""));
        }

        context.write(new Text("Off-White: "), new Text(""));
        Set<Long> longs2 = treeMap2.keySet();
        for (Long aLong : longs2) {
            context.write(new Text(aLong + "\t" + treeMap2.get(aLong)), new Text(""));
        }
    }
}

public class shixun6 {
    //    6.每种品牌销售前三的鞋的种类
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

        String input = "/shixun/datas/newSneaker_data.csv";
        String output = "/shixun/output6";

        //设置job名称
        job.setJobName("Job3_shixun6");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(shixun6.class);

        //map函数位置
        job.setMapperClass(map6.class);
//        job.setCombinerClass(reduce.class);
        job.setReducerClass(reduce6.class);

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
