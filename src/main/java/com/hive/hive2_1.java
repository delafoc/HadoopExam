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
import java.net.URI;
import java.util.HashMap;
import java.util.Set;

class map3 extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        byte[] bytes = value.getBytes();
        String str = new String(bytes, 0, value.getLength(), "GBK");
        String[] strings = str.trim().split(",");
        String[] colors = new String[]{"红色", "黑色", "白色", "灰色", "蓝色", "橙色", "绿色", "紫色", "粉色", "黄色"};
        String ys = colors[(int)Math.floor(Math.random()*10)];
        if (strings!=null&&strings.length>38&&strings[38]!=""&&strings[38]!=null)
            context.write(new Text(strings[38] + "\t" + ys), new LongWritable(1));
    }
}

class reduce3 extends Reducer<Text, LongWritable, Text, DoubleWritable> {
    HashMap<String, Long> map = new HashMap<String, Long>();
    int n = 0;
    double num = 0;//总车辆数
    long man = 0, woman = 0;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        long sum = 0;
        String s = key.toString();
        String[] split = s.split("\t");

        for (LongWritable value : values) {
            sum += value.get();
        }
        if (split[0].trim().equals("男性")){
            man += sum;
            num += sum;
            if (map.get(s) != null) {
                Long tmp = map.get(s) + sum;
                map.remove(s);
                map.put(s, tmp);
            } else {
                map.put(s, sum);
            }
        } else if (split[0].trim().equals("女性")){
            woman += sum;
            num += sum;
            if (map.get(s) != null) {
                Long tmp = map.get(s) + sum;
                map.remove(s);
                map.put(s, tmp);
            } else {
                map.put(s, sum);
            }
        }

    }

    @Override
    protected void cleanup(Reducer<Text, LongWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

        if (n == 0) {
            context.write(new Text("男性的数量为：" + man + "  比列为： " + man / num + "\n"
                    + "女性的数量为：" + woman + "  比列为："), new DoubleWritable(woman / num));
            n++;
        }

        Set<String> keySet = map.keySet();
        for (String s : keySet) {
            Long value = map.get(s);
            double percent = value / num;
            context.write(new Text(s + "销售数量的比例为："), new DoubleWritable(percent));
        }
    }
}

public class hive2_1 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

//        String input = "/hivetest/test/11 汽车销售数据-Cars.csv";
        String input = "/hivetest/cars.csv";
        String output = "/hivetest/output/test2.1";

        //设置job名称
        job.setJobName("hive2.1");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(hive2_1.class);

        //map函数位置
        job.setMapperClass(map3.class);
        job.setReducerClass(reduce3.class);

        //map设置出来的
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

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

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job
        boolean result = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
        System.exit(result ? 0 : -1);
    }
}
