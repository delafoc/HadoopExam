import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class chuangjian {
    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //找到Hadoop人口
//        URI uri = new URI("hdfs://192.168.136.193:9000");
        URI uri = new URI("hdfs://192.168.88.130:9000");
        //创建配置文件
        Configuration conf = new Configuration();
        conf.set("dfs.replication","1");
        //设置用户
        String usr = "hadoop";
        //创建FileSystem对象
        fs = FileSystem.get(uri, conf, usr);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        //创建操作
        fs.mkdirs(new Path("/demo"));
    }

    @Test
    public void testUpload() throws URISyntaxException, IOException, InterruptedException {
        //上传操作
        fs.copyFromLocalFile(false, true, new Path("E:\\程序\\PyCharmWorkStation\\pyspark\\data\\input\\words.txt"), new Path("/input/words.txt"));
    }

    @Test
    public void xiaZai() throws IOException {
        //下载操作
        for (int i = 0; i < 7; i ++ ) {
            String str = "output" + (i+1);
            fs.copyToLocalFile(true,new Path("/shixun/asd"+ str),new Path("C:\\Users\\Undefeated604\\Desktop\\统计数据\\"),true);
        }
    }

    @Test
    public void moveFiles() throws IOException {
        //移动的重命名
        fs.rename(new Path("/work/homework/demo2.txt"),new Path("/work/homework/demo3.txt"));
    }

    @Test
    public void deleteFiles() throws IOException {
        //删除操作
        fs.delete(new Path("/user/hive/warehouse/lulingzhi.db/sneaker_data/Shoes"),true);
    }

    @Test
    public void fileprint1() throws URISyntaxException, IOException, InterruptedException {
        //打印快信息操作
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/work/homework/demo2.txt"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
        }
    }

    @Test
    public void fileprint2() throws URISyntaxException, IOException, InterruptedException {
        //操作
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/work/homework/demo2.txt"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void file() throws URISyntaxException, IOException, InterruptedException {
        //操作
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/work/homework/demo2.txt"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
    }

}
