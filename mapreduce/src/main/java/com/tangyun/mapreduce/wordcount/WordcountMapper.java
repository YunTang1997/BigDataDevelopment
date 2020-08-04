package com.tangyun.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author YunTang
 * @create 2020-06-27 9:55
 */

// map阶段
// KEYIN（第一个参数） 输入数据的key（偏移量）
// VALUEIN（第二个参数） 输入数据的value（每一行的文本数据）
// KEYOUT（第三个参数） 输出数据的key（单词名）
// VALUEOUT（第四个参数） 输出数据的value（单词次数统计）
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1.获取一行
        String line = value.toString();

        // 2.切割单词（空格）
        String[] words = line.split(" ");

        // 3.循环写出
        for (String word : words) {
            k.set(word); // k取值为word
            context.write(k, v);

        }
    }
}
