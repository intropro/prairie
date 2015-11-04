package com.intropro.prairie.unit.oozie.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by presidentio on 9/21/15.
 */
public class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for (String s : value.toString().split("\\s")) {
            context.write(new Text(s), new IntWritable(1));
        }
    }
}
