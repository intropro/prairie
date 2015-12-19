package com.intropro.prairie.benchmarks.hdfs;

import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.exception.PrairieException;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 10/27/15.
 */
@Fork(3)
@Warmup(iterations = 0)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SampleTime)
@State(Scope.Thread)
@Threads(1)
public class HdfsStopBenchmark {

    private HdfsUnitContainer hdfsUnitContainer;
    private DependencyResolver dependencyResolver;

    @Setup(Level.Invocation)
    public void init() throws PrairieException {
        hdfsUnitContainer = new HdfsUnitContainer();
        dependencyResolver = new DependencyResolver();
        dependencyResolver.resolve(hdfsUnitContainer);
    }

    @Benchmark
    public void measureStop() throws PrairieException {
        dependencyResolver.destroy(hdfsUnitContainer);
    }

}
