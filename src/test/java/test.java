
import java.io.IOException;

import com.jcraft.jsch.IO;
import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class test {
    static int sum = 0;
    static class class_map extends Mapper<LongWritable, Text,Text, Text> {
        Text uid = new Text();
        Text record = new Text();
        @Override
        protected void map(LongWritable int1,Text int2, Context context)throws IOException,InterruptedException {
            String line = int2.toString();
            System.out.println(int2);
            String[] now = line.split("\t");
            uid.set(now[1]);
            record.set(now[2]);
            if(record.toString().contains("仙剑奇侠传"))
                context.write(record,uid);
        }
    }
    static class class_reduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key,Iterable<Text> v,Context context) throws IOException, InterruptedException {
            for(Text x:v){
                context.write(key,x);
            }
        }
    }
    static class map2 extends Mapper<LongWritable,Text,Text,IntWritable>{
        Text uid = new Text();
        @Override
        protected void map(LongWritable k,Text str,Context context)throws IOException,InterruptedException{
            String line = str.toString();
            String[] now = line.split("\t");
            int rank = Integer.parseInt(now[3]);
            int order = Integer.parseInt(now[4]);
            uid.set(now[1]);
            if(rank < 3 && order > 2){
                context.write(uid,new IntWritable(1));
            }
        }
    }
    static class reduce2 extends Reducer<Text,IntWritable,IntWritable,NullWritable>{
        @Override
        protected void reduce(Text key,Iterable<IntWritable> v,Context context) throws IOException,InterruptedException{
            sum++;
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(sum),null);
        }
    }
    static class map3 extends Mapper<LongWritable,Text,Text,Text>{
        Text uid = new Text();
        Text keyword = new Text();
        @Override
        protected void map(LongWritable key,Text in,Context context) throws IOException, InterruptedException {
            String line = in.toString();
            String[] now = line.split("\t");
            int time = Integer.parseInt(now[0].substring(8,10));
            keyword.set(now[2]);
            uid.set(now[1]);
            if(time >= 7 && time <= 9 && now[2].contains("赶集网")){
                context.write(uid,keyword);
            }
        }
    }
    static class reduce3 extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key,Iterable<Text> v,Context context) throws IOException,InterruptedException{
            for(Text x:v){
                context.write(key,x);
            }
        }
    }
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf);
        job.setJar("E:\\程序\\Java\\hadoopexam\\target\\hadoopexam-1.0-SNAPSHOT.jar");
        job.setJarByClass(test.class);
        job.setJobName("IQY2");
        FileInputFormat.setInputPaths(job,new Path("/input/sogou.500w.utf8"));
        FileOutputFormat.setOutputPath(job,new Path("/output"));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(class_map.class);
        job.setReducerClass(class_reduce.class);
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);
    }
}
