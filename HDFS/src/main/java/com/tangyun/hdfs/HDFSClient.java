package com.tangyun.hdfs;

/**
 * @author YunTang
 * @create 2020-06-20 13:29
 */

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSClient {

    public static void main(String[] args) throws IOException, Exception, URISyntaxException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        // 1获取hdfs客户端对象
        // FileSystem fs = FileSystem.get(conf );
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "tangyun");

        // 2在hdfs上创建路径
        fs.mkdirs(new Path("/wwllqq/test/dashen"));

        // 3关闭资源
        fs.close();

        System.out.println("over");
    }

    // 1 文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2执行上传API
        fs.copyFromLocalFile(new Path("C:/Users/tangyun/Desktop/test2.txt"), new Path("/test3.txt"));

        // 3关闭资源
        fs.close();
    }

    // 2文件下载
    @Test
    public void testCopyTolocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2执行下载API
//		fs.copyToLocalFile(new Path("/test3.txt"), new Path("C:/Users/tangyun/Desktop"));
        fs.copyToLocalFile(false, new Path("/test3.txt"), new Path("C:/Users/tangyun/Desktop/hah.txt"), true);
        // 3关闭资源
        fs.close();
    }

    // 3文件删除
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2文件删除
        fs.delete(new Path("/test3.txt"), false);
        // 3关闭资源
        fs.close();
    }

    // 4文件更名
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException {
        // 1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2文件删除
        fs.rename(new Path("/test2.txt"), new Path("/test_new.txt"));
        // 3关闭资源
        fs.close();
    }

    // 文件详情查看
    @Test
    public void testListFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            // 查看文件名称、权限、长度、块信息
            System.out.println(fileStatus.getPath().getName()); // 文件名称
            System.out.println(fileStatus.getPermission()); // 文件权限
            System.out.println(fileStatus.getLen()); //文件长度

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);

                }
            }
            System.out.println("--------test分割线--------");
        }

        // 3关闭资源
        fs.close();
    }

    // 6判断试文件还是文件夹
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取对象

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2判断操作
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                // 文件
                System.out.println("f:" + fileStatus.getPath().getName());
            }
            else {
                // 文件夹
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        // 3关闭资源
        fs.close();
    }


}
