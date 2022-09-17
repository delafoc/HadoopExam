package com.hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

class map8 extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        byte[] bytes = value.getBytes();
        String str = new String(bytes, 0, value.getLength(), "GBK");
        String[] strings = str.trim().split(",");
        if (strings!=null&&strings.length>28&&strings[7].trim().length()>0&&strings[27].trim().length()>0) {
            context.write(new Text(strings[27] + "\t" + strings[7]), new LongWritable(1));
        }
    }
}

class reduce8 extends Reducer<Text, LongWritable, Text, Text> {
    HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
    double num = 0;//总车辆数

    @Override
    protected void setup(Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("价格" + "\t" + "品牌" + "\t" + "数量" + "\t" + "比例"), new Text(""));
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        long sum = 0;
        String[] split = key.toString().split("\t");//价格和品牌
        for (LongWritable value : values) {
            sum += value.get();
            num += sum;
        }
        if (map.get(split[0])!=null){
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
            for (String s1 : list) {
                String[] split = s1.split("-");
                try{
                    int n = Integer.parseInt(split[1]);
                    double percent = n / num;
                    context.write(new Text(s + "\t" + split[0] + "\t" + n + "\t" + percent), new Text(""));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

public class hive3_3 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

//        String input = "/hivetest/test/11 汽车销售数据-Cars.csv";
        String input = "/hivetest/cars.csv";
        String output = "/hivetest/output/test3.3";

        //设置job名称
        job.setJobName("hive3.3");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(hive3_3.class);

        //map函数位置
        job.setMapperClass(map8.class);
        job.setReducerClass(reduce8.class);

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
