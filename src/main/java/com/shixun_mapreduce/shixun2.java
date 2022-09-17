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
import java.util.HashMap;
import java.util.Set;

class map2 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().trim().split(",");
        if (strings[2].length() > 0 && strings[8].length() > 0)
            context.write(new Text(strings[2]), new Text(strings[8]));
    }
}

class reduce2 extends Reducer<Text, Text, Text, Text> {
    HashMap<String, Long> yeezyMap = new HashMap<String, Long>(); // yeezy
    HashMap<String, Long> off_whiteMap = new HashMap<String, Long>(); // off_white
    int n = 0;
    int yeezy = 0, off_white = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        if (key.toString().contains("Yeezy")) {
            for (Text value : values) {
                if (yeezyMap.get(value.toString()) == null) {
                    yeezyMap.put(value.toString(), new Long("1"));
                } else {
                    Long amount = yeezyMap.get(value.toString());
                    amount++;
                    yeezyMap.remove(value.toString());
                    yeezyMap.put(value.toString(), amount);
                }
                yeezy++;
                n++;
            }
        } else {
            for (Text value : values) {
                if (off_whiteMap.get(value.toString()) == null) {
                    off_whiteMap.put(value.toString(), new Long("1"));
                } else {
                    Long amount = off_whiteMap.get(value.toString());
                    amount++;
                    off_whiteMap.remove(value.toString());
                    off_whiteMap.put(value.toString(), amount);
                }
                off_white++;
                n++;
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("Yeezy总销售量为：" + yeezy + "\t\n" + "Yeezy不同地区销售情况："), new Text(""));
        context.write(new Text("品牌" + "\t" + "地区" + "\t" + "销售量"), new Text(""));
        Set<String> yeezyKeySet = yeezyMap.keySet();
        for (String s : yeezyKeySet) {
            context.write(new Text("Yeezy" + ",\"" + s + "\"," + yeezyMap.get(s)), new Text(""));
        }
        context.write(new Text("Off-White总销售量为：" + off_white + "\t\n" + "Off-White不同地区销售情况："), new Text(""));
        context.write(new Text("品牌" + "\t" + "地区" + "\t" + "销售量"), new Text(""));
        Set<String> off_whiteKeySet = off_whiteMap.keySet();
        for (String s : off_whiteKeySet) {
            context.write(new Text("Off-White" + ",\"" + s + "\"," + off_whiteMap.get(s)), new Text(""));
        }
    }
}

public class shixun2 {
    //    2.每种品牌销售地区分布与数量统计
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

        String input = "/shixun/datas/newSneaker_data.csv";
        String output = "/shixun/output2";

        //设置job名称
        job.setJobName("Job3_shixun2");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(shixun2.class);

        //map函数位置
        job.setMapperClass(map2.class);
//        job.setCombinerClass(reduce.class);
        job.setReducerClass(reduce2.class);

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
