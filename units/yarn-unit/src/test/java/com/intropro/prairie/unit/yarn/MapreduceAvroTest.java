package com.intropro.prairie.unit.yarn;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.EntryComparator;
import com.intropro.prairie.format.avro.AvroFormat;
import com.intropro.prairie.format.json.JsonFormat;
import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * Created by presidentio on 04.09.15.
 */
@RunWith(PrairieRunner.class)
public class MapreduceAvroTest {

    private String inputPath = "/MapreduceAvroTest/input";
    private String outputPath = "/MapreduceAvroTest/output";

    private EntryComparator<String> byLineComparator = new ByLineComparator<String>();

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @PrairieUnit
    private YarnUnit yarnUnit;

    @Test
    public void testRunAvroJob() throws Exception {
        hdfsUnit.getFileSystem().mkdirs(new Path(inputPath));
        hdfsUnit.getFileSystem().mkdirs(new Path(outputPath).getParent());
        hdfsUnit.saveAs(MapreduceAvroTest.class.getClassLoader().getResourceAsStream("mapreduce-avro/avro-input.json"),
                inputPath + "/part-00000", new JsonFormat(), new AvroFormat(
                        MapreduceAvroTest.class.getClassLoader().getResourceAsStream("mapreduce-avro/input.avsc")));
        createAndSubmitJob();
        hdfsUnit.compare(new Path(outputPath), new AvroFormat(
                        MapreduceAvroTest.class.getClassLoader().getResourceAsStream("mapreduce-avro/output.avsc")),
                MapreduceAvroTest.class.getClassLoader().getResourceAsStream("mapreduce-avro/avro-output.json"),
                new JsonFormat()).assertEquals();
    }

    public boolean createAndSubmitJob() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration(yarnUnit.getConfig());
        configuration.setBoolean("mapred.mapper.new-api", true);
        configuration.setBoolean("mapred.reducer.new-api", true);
        Job job = Job.getInstance(configuration);
        job.setJobName(this.getClass().getSimpleName() + "-job");

        job.setNumReduceTasks(1);

        job.setMapperClass(AvroMapReduce.AvroMapper.class);

        Schema inputSchema = new Schema.Parser().parse(
                MapreduceAvroTest.class.getClassLoader().getResourceAsStream("mapreduce-avro/input.avsc"));
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, inputSchema);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AvroMapReduce.AvroReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job, new Schema.Parser().parse(
                MapreduceAvroTest.class.getClassLoader().getResourceAsStream("mapreduce-avro/output.avsc")));
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);

        job.setSpeculativeExecution(false);
        job.setMaxMapAttempts(1); // speed up failures
        return job.waitForCompletion(true);
    }
}