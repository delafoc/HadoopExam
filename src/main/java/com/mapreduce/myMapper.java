package com.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /*
    20111230000005	   									Time：用户访问时间
    57375476989eea12893c0c3811607bcf	 				Uid：用户的id
    奇艺高清 											Keyword：访问的关键词
    1 													Rank：点击排名
    1                                                   Order：页数
    http://www.qiyi.com/                                Url：网址
     */
//    String Time = new String();
//    String Uid = new String();
//    String Keyword = new String();
//    String Rank = new String();
//    String Order = new String();
//    String Url = new String();
//    Text keyOut = new Text();
//    Text valueOut = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1，统计出搜索过包含有“仙剑奇侠传”内容的UID及搜索关键字记录

        //获取每一行内容
        String line = value.toString();
        //按照对应的分隔符将数据切分得到数组
        String[] words = line.split("\t");
        //遍历数据数组，出现的就记为1
        //for (String word : words) {
        //context.write(new Text(word), new IntWritable(1));
        //}
        //遍历数据数组并进行操作
//        if (words.length == 6) {
//            Time = words[0];
//            Uid = words[1];
//            Keyword = words[2];
//            Rank = words[3];
//            Order = words[4];
//            Url = words[5];
//        } else System.out.println("数据错误");

//        if (Keyword.contains("仙剑奇侠传")) {
//            keyOut.set(Keyword);
//            valueOut.set(Uid);
//            context.write(keyOut, valueOut);
//        }
        if (words[2].contains("仙剑奇侠传")) {
            context.write(new Text(words[2] + "\t" + words[1]), new IntWritable(1));
        }
    }
}
