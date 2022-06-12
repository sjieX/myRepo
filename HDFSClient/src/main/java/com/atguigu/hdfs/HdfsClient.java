package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 1、获取一个客户端对象
 * 2.执行相关的操作命令
 * 3.关闭资源
 * <p>
 * HDFS  zookeeper
 **/
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群的nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件,可设置配置文件中副本数等配置，优先级最高
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");

        // 用户
        String user = "xinshujie";

        // 1 获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }

    // 创建目录
    @Test
    public void testmkdir() throws IOException {
        // 2 创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    // 上传

    /**
     * 参数优先级排序：（1）客户端代码Configuration设置的值 >（2）项目资源目录下的配置文件 >
     * （3）然后是服务器的自定义配置（xxx-site.xml） >（4）服务器的默认配置（xxx-default.xml）
     **/
    @Test
    public void testPut() throws IOException {
        // 参数解读   参数一：上传完毕后是否删除原数据  参数二：是否允许覆盖  参数三：原数据路径   参数四：目的路径
        fs.copyFromLocalFile(false, true, new Path("D:\\sunwukong.txt"), new Path("/xiyou/huaguoshan/sunwukong.txt"));
    }

    // 下载
    @Test
    public void testGet() throws IOException {
        // 参数的解读  参数一：原文件是否删除  参数二：原文件路径HDFS  参数三：目标地址Win  参数四：是否开启本地CRC校验（false开启）
        fs.copyToLocalFile(true, new Path("/xiyou/huaguoshan"), new Path("D://"), true);
    }

    // 删除
    @Test
    public void testRm() throws IOException {
        // 参数解读    参数一：要删除的路径HDFS  参数二：是否递归删除
        // 删除文件
        fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz"), false);

        // 删除空目录
        fs.delete(new Path("/xiyou"), false);

        // 删除非空目录(递归删除)
        fs.delete(new Path("/tmp"), true);
    }

    // 文件更名和移动
    @Test
    public void testmv() throws IOException {
        // 参数解读    参数一：原文件路径   参数二：目标文件路径
        // 对文件名进行修改
        fs.rename(new Path("/input/word.txt"), new Path("/input/word2.txt"));

        //文件的移动和更名
        fs.rename(new Path("/input/word2.txt"), new Path("/word3.txt"));

        // 对目录名进行修改
        fs.rename(new Path("/input"), new Path("/output"));
    }

    // 获取文件详细信息
    @Test
    public void fileDetail() throws IOException {
        // 参数解读   参数一：查看的路径    参数二：是否递归
        // 获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        // 遍历迭代器，输出信息
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("==========" + fileStatus.getPath() + "============");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println(Arrays.toString(blockLocations));

        }

    }

    // 文件和文件夹的判断
    @Test
    public void testFile() throws IOException {

        // 要判断的路径
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件："+status.getPath().getName());
            }else {
                System.out.println("目录："+status.getPath().getName());
            }
        }

    }

}
