package com.intropro.prairie.unit.yarn;

import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.CompareResponse;
import com.intropro.prairie.comparator.EntryComparator;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

/**
 * Created by presidentio on 04.09.15.
 */
@RunWith(PrairieRunner.class)
public class YarnUnitTest {

    private String input = "some text for count job\nwith text";
    private String expectedOutput = "some\t1\ntext\t2\nfor\t1\ncount\t1\njob\t1\nwith\t1";
    private String inputPath = "/YarnUnitTest/input";
    private String outputPath = "/YarnUnitTest/output";

    private EntryComparator<String> byLineComparator = new ByLineComparator<String>();

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @PrairieUnit
    private YarnUnit yarnUnit;

    @Test
    public void testRunEmptyJob() throws Exception {
        hdfsUnit.getFileSystem().mkdirs(new Path(inputPath));
        FSDataOutputStream outputStream = hdfsUnit.getFileSystem().create(new Path(inputPath, "part-00000"));
        outputStream.writeBytes(input);
        outputStream.flush();
        outputStream.close();
        createAndSubmitJob();
        FSDataInputStream inputStream = hdfsUnit.getFileSystem().open(new Path(outputPath, "part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        List<String> outputLines = IOUtils.readLines(br);
        inputStream.close();
        List<String> expectedOutputLines = Arrays.asList(expectedOutput.split("\n"));
        CompareResponse compareResponse = byLineComparator.compare(expectedOutputLines, outputLines);
        Assert.assertTrue("Unexpected lines: " + compareResponse.getUnexpected(), compareResponse.getUnexpected().isEmpty());
        Assert.assertTrue("Missed lines: " + compareResponse.getMissed(), compareResponse.getMissed().isEmpty());
    }

    public boolean createAndSubmitJob() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(yarnUnit.getConfig());
        job.setJobName(this.getClass().getSimpleName() + "-job");

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CountMapReduce.CountMapper.class);
        job.setReducerClass(CountMapReduce.CountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setSpeculativeExecution(false);
        job.setMaxMapAttempts(1);
        return job.waitForCompletion(true);
    }
}