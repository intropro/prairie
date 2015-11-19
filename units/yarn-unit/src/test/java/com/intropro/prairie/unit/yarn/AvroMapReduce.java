package com.intropro.prairie.unit.yarn;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by presidentio on 11/19/15.
 */
public class AvroMapReduce {

    public static class AvroMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, IntWritable, Text> {

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            String sum = "";
            sum += (String) key.datum().get("field1");
            sum += (String) key.datum().get("field2");
            sum += (String) key.datum().get("field3");
            sum += (String) key.datum().get("field4");
            sum += (String) key.datum().get("field5");
            context.write(new IntWritable(0), new Text(sum));
        }
    }

    public static class AvroReducer extends Reducer<IntWritable, Text, AvroKey<GenericRecord>, NullWritable> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            schema = new Schema.Parser().parse(context.getConfiguration().get("avro.schema.output.key"));
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            GenericRecord genericRecord = new GenericData.Record(schema);
            String sum = "";
            for (Text value : values) {
                sum += value.toString();
            }
            genericRecord.put("field_sum", sum);
            context.write(new AvroKey<>(genericRecord), NullWritable.get());
        }
    }
}
