package com.hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

class myDate implements WritableComparable<myDate> {
    Integer year;
    Integer month;

    public myDate() {
    }

    public myDate(Integer year, Integer month) {
        this.year = year;
        this.month = month;
    }

    public myDate(String year, String month) {
        this.year = Integer.parseInt(year);
        this.month = Integer.parseInt(month);
    }

    @Override
    public int compareTo(myDate o) {
        int result = year.compareTo(o.year);
        if (result != 0) return result;
        return month.compareTo(o.month);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        month = in.readInt();
    }

    @Override
    public String toString() {
        return year + "-" + month;
    }
}

class map9 extends Mapper<LongWritable, Text, myDate, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, myDate, LongWritable>.Context context) throws IOException, InterruptedException {
        byte[] bytes = value.getBytes();
        String str = new String(bytes, 0, value.getLength(), "GBK");
        String[] strings = str.trim().split(",");
        if (strings != null && strings.length > 7 && strings[7].trim().length() > 0 && strings[1].trim().length() > 0 && strings[4].trim().length() > 0) {
            if (strings[7].trim().contains("五菱")) {
                context.write(new myDate(strings[4], strings[1]), new LongWritable(1));
            }
        }
    }
}

class reduce9 extends Reducer<myDate, LongWritable, Text, Text> {
    HashMap<String, String> map = new HashMap<String, String>();
    double num = 0;//总车辆数
    double q = 0;
    int n = 0;

    @Override
    protected void setup(Reducer<myDate, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("日期" + "\t" + "销售量" + "\t" + "较上月增长率"), new Text(""));
    }

    @Override
    protected void reduce(myDate key, Iterable<LongWritable> values, Reducer<myDate, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        num += sum;
        if (n == 0) {
            map.put(key.toString(), sum + "\t" + 0);
            n++;
        }
        else map.put(key.toString(), sum + "\t" + (sum-q)/q);
        q = sum;
    }

    @Override
    protected void cleanup(Reducer<myDate, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> keySet = map.keySet();
        for (String s : keySet) {
            String s2 = map.get(s);
            context.write(new Text(s), new Text(s2));
        }
    }
}

public class hive4_1 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

//        String input = "/hivetest/test/11 汽车销售数据-Cars.csv";
        String input = "/hivetest/cars.csv";
        String output = "/hivetest/output/test4.1";

        //设置job名称
        job.setJobName("hive4.1");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(hive4_1.class);

        //map函数位置
        job.setMapperClass(map9.class);
        job.setReducerClass(reduce9.class);

        //map设置出来的
        job.setMapOutputKeyClass(myDate.class);
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
