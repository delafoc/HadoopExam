package com.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper3 extends Mapper<LongWritable, Text, Text, IntWritable> {
    /*
    20111230000005	   									Time：用户访问时间
    57375476989eea12893c0c3811607bcf	 				Uid：用户的id
    奇艺高清 											Keyword：访问的关键词
    1 													Rank：点击排名
    1                                                   Order：页数
    http://www.qiyi.com/                                Url：网址
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //3.上午7-9点之间，搜索过“赶集网”的用户UID

        //获取每一行内容
        String line = value.toString();
        //按照对应的分隔符将数据切分得到数组
        String[] words = line.split("\t");
        //遍历数据数组，出现的就记为1
        String s = words[0].substring(8, 10);
        int time = Integer.parseInt(s);
        System.out.println(time);
        if (time>=7 && time<=9 && words[2].contains("赶集网"))
            context.write(new Text(words[1] + "\t" + words[2]), new IntWritable(1));
    }
}
