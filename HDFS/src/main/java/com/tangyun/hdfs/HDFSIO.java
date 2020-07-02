package com.tangyun.hdfs;

/**
 * @author YunTang
 * @create 2020-06-20 13:32
 */


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


public class HDFSIO {

    // 将本地文件上传到HDFS根目录
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2获取输入流
        FileInputStream fis = new FileInputStream(new File("C:/Users/tangyun/Desktop/test3.txt"));

        // 3获取输出流
        FSDataOutputStream fos = fs.create(new Path("/test3.txt"));

        // 4流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    // 下载文件到本地
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "tangyun");

        // 2获取输入流
        FSDataInputStream fis = fs.open(new Path("/test3.txt"));

        // 3获取输出流
        FileOutputStream fos = new FileOutputStream(new File("C:/Users/tangyun/Desktop/test4.txt"));

        // 4流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    // 文件的定位读取
    @SuppressWarnings("resource")
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException {
        // 1获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3获取输出流
        FileOutputStream fos = new FileOutputStream(new File("C:/Users/tangyun/Desktop/hadoop-2.7.2.tar.gz.part1"));

        // 4流的对拷（只拷128M）
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);

        }

        // 5关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    // 下载第二块
    @SuppressWarnings("resource")
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {
        // 1获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf , "tangyun");

        // 2获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3设置指定读取的起点
        fis.seek(1024 * 1024 * 128);

        // 4获取输出流
        FileOutputStream fos = new FileOutputStream(new File("C:/Users/tangyun/Desktop/hadoop-2.7.2.tar.gz.part2"));

        // 5流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 6关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
