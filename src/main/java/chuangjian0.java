import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class chuangjian0 {
    @Test
    public void test() throws URISyntaxException, IOException, InterruptedException {
    //给入口网址
    URI uri=new URI("hdfs://192.168.94.193:9000");
    //创建配置文件
    Configuration conf = new Configuration();
    //给用户名称修改成自己的用户名
    String user="hadoop";
    //创建对象
    FileSystem fs = FileSystem.get(uri,conf,user);
    //操作
    fs.mkdirs(new Path("/liukang"));
    //关闭
    fs.close();
    }
}
