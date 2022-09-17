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

class map extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().trim().split(",");
            context.write(new Text(strings[2]), new Text(strings[1] + "_" + strings[5].trim().replace("$", "")));
    }
}

class reduce extends Reducer<Text, Text, Text, Text> {
    HashMap<String, ArrayList<Double>> yeezyMap = new HashMap<String, ArrayList<Double>>(); // yeezy
    HashMap<String, ArrayList<Double>> off_whiteMap = new HashMap<String, ArrayList<Double>>(); // off_white
    int n = 0;
    int yeezy = 0, off_white = 0;

    public void addIntoMap(String[] s, HashMap<String, ArrayList<Double>> map) {
        String date = s[0];
        String price = s[1];
        String[] dates = date.split("/");
        String year = dates[0];
        if (Integer.parseInt(year) < 10) {
            year = "200" + year;
        } else if (Integer.parseInt(year) < 100) {
            year = "20" + year;
        }
        if (map.get(year) == null) {
            ArrayList<Double> prices = new ArrayList<>();
            prices.add(Double.parseDouble(price));
            map.put(year, prices);
        } else {
            ArrayList<Double> priceList = map.get(year);
            priceList.add(Double.parseDouble(price));
            map.remove(year);
            map.put(year, priceList);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        if (key.toString().contains("Yeezy")) {
            for (Text value : values) {
                String[] s = value.toString().split("_");
                addIntoMap(s, yeezyMap);
                yeezy++;
                n++;
            }
        } else {
            for (Text value : values) {
                String[] s = value.toString().split("_");
                addIntoMap(s, off_whiteMap);
                off_white++;
                n++;
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("Yeezy总销售量为：" + yeezy + "\t\n" + "Yeezy每年销售情况："), new Text(""));
        context.write(new Text("年份" + "\t" + "销售量" + "\t" + "销售额"), new Text(""));
        Set<String> yeezyKeySet = yeezyMap.keySet();
        for (String s : yeezyKeySet) {
            ArrayList<Double> doubles = yeezyMap.get(s);
            long yeezyNum = 0;
            double yeezyMoney = 0;
            for (Double aDouble : doubles) {
                yeezyMoney += aDouble;
                yeezyNum++;
            }
            context.write(new Text(s + "\t" + yeezyNum + "\t" + yeezyMoney), new Text(""));
        }
        context.write(new Text("Off-White总销售量为：" + off_white + "\t\n" + "Off-White每年销售情况："), new Text(""));
        context.write(new Text("年份" + "\t" + "销售量" + "\t" + "销售额"), new Text(""));
        Set<String> off_whiteKeySet = off_whiteMap.keySet();
        for (String s : off_whiteKeySet) {
            ArrayList<Double> doubles = off_whiteMap.get(s);
            long off_whiteNum = 0;
            double off_whiteMoney = 0;
            for (Double aDouble : doubles) {
                off_whiteMoney += aDouble;
                off_whiteNum++;
            }
            context.write(new Text(s + "\t" + off_whiteNum + "\t" + off_whiteMoney), new Text(""));
        }
    }
}

public class shixun1 {
    //    1.每种品牌每一年的销售量和销售额
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");  // 使用Hadoop用户权限

        // 创建job实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);

        String input = "/shixun/datas/newSneaker_data.csv";
        String output = "/shixun/output1";

        //设置job名称
        job.setJobName("Job3_shixun1");

        //set创建Jar包位置
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");

        //主函数位置
        job.setJarByClass(shixun1.class);

        //map函数位置
        job.setMapperClass(map.class);
//        job.setCombinerClass(reduce.class);
        job.setReducerClass(reduce.class);

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
