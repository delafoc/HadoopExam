package com.hive;


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

class map10 extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        byte[] bytes = value.getBytes();
        String str = new String(bytes, 0, value.getLength(), "GBK");
        String[] strings = str.trim().split(",");
        if (strings != null && strings.length > 28 && strings[0].trim().length() > 0 && strings[27].trim().length() > 0) {
            try {
                context.write(new Text(strings[0] + "-" + Integer.parseInt(strings[27])), new LongWritable(1));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
    }
}

class reduce10 extends Reducer<Text, LongWritable, Text, Text> {
    HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();

    @Override
    protected void setup(Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("省份" + "\t" + "销售量" + "\t" + "价格差异" + "\t" + "总销售额"), new Text(""));
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        long sum = 0;
        String[] split = key.toString().split("-");//价格和品牌
        for (LongWritable value : values) {
            sum += value.get();
        }
        if (map.get(split[0]) != null) {
            ArrayList<String> strings = map.get(split[0]);
            strings.add(split[1] + "-" + sum);
            map.remove(split[0]);
            map.put(split[0], strings);
        } else {
            ArrayList<String> array = new ArrayList<>();
            array.add(split[1] + "-" + sum);
            map.put(split[0], array);
        }
    }

    @Override
    protected void cleanup(Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> keySet = map.keySet();
        for (String s : keySet) {
            ArrayList<String> list = map.get(s);
            ArrayList<String> prices = new ArrayList<>();
            long total = 0;
            long allNum = 0;
            for (String s1 : list) {
                String[] split = s1.split("-");
                try {
                    int price = Integer.parseInt(split[0]);
                    int number = Integer.parseInt(split[1]);
                    allNum += number;
                    total += price * number;
                    prices.add("" + price);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
            context.write(new Text(s + "\t" + allNum + "\t" + prices.toString() + "\t" + total), new Text(""));
        }
    }
}

public class hive4_2 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

//        String input = "/hivetest/test/11 汽车销售数据-Cars.csv";
        String input = "/hivetest/cars.csv";
        String output = "/hivetest/output/test4.2";

        //设置job名称
        job.setJobName("hive4.2");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(hive4_2.class);

        //map函数位置
        job.setMapperClass(map10.class);
        job.setReducerClass(reduce10.class);

        //map设置出来的
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

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
