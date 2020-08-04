package com.tangyun.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author YunTang
 * @create 2020-08-04 19:40
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1.获取一行
        // 8 	15910133277	192.168.100.5	www.hao123.com	3156	2936	200
        String line = value.toString();

        // 2.按"\t"分割
        String[] fileds = line.split("\t");

        // 3.封装对象
        k.set(fileds[1]); // 封装手机号
        long upFlow = Long.parseLong(fileds[fileds.length - 3]);
        long downFlow = Long.parseLong(fileds[fileds.length - 2]);

        v.setUpFlow(upFlow); // 封装上流量
        v.setDownFlow(downFlow); // 封装下流量

        // 4.写出
        context.write(k, v);

    }
}
