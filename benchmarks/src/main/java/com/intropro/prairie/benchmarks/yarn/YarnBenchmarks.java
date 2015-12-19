package com.intropro.prairie.benchmarks.yarn;

import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.PrairieException;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.yarn.YarnUnit;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 12/14/15.
 */
@Fork(2)
@Warmup(iterations = 0)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.SampleTime)
@State(Scope.Thread)
@Threads(1)
public class YarnBenchmarks {

    private String outputPath = "/YarnUnitTest/output";
    private String inputPath = "/YarnUnitTest/input";
    private String input = "some text for count job\nwith text";

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @PrairieUnit
    private YarnUnit yarnUnit;

    private DependencyResolver dependencyResolver;

    @Setup(Level.Invocation)
    public void init() throws PrairieException, IOException {
        dependencyResolver = new DependencyResolver();
        dependencyResolver.resolve(this);
        hdfsUnit.getFileSystem().mkdirs(new Path(inputPath));
        FSDataOutputStream outputStream = hdfsUnit.getFileSystem().create(new Path(inputPath, "part-00000"));
        outputStream.writeBytes(input);
        outputStream.flush();
        outputStream.close();
    }

    @Benchmark
    public void measureRun() throws PrairieException, InterruptedException, ClassNotFoundException, IOException {
        createAndSubmitJob();

    }

    @TearDown(Level.Invocation)
    public void destroy() throws DestroyUnitException {
        dependencyResolver.destroy(this);
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
