package com.tangyun.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author YunTang
 * @create 2020-08-03 21:00
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        int sum = 0;
        // 1.累加求和
        for (IntWritable value : values) {

            sum += value.get();
        }

        IntWritable v = new IntWritable();
        v.set(sum);
        // 2.写出
        context.write(key, v);
    }
}
