package com.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
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
        //2，统计rank<3并且order>2的所有UID及数量

        //获取每一行内容
        String line = value.toString();
        //按照对应的分隔符将数据切分得到数组
        String[] words = line.split("\t");
        //遍历数据数组，出现的就记为1
        int rank = Integer.parseInt(words[3]);
        int order = Integer.parseInt(words[4]);
        if (rank < 3 && order > 2)
            context.write(new Text(words[1]), new IntWritable(1));
    }
}
