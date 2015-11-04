package com.intropro.prairie.benchmarks.flume;

import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.exception.BigDataTestFrameworkException;
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
public class FlumeStopBenchmark {

    private FlumeUnitContainer flumeUnitContainer;
    private DependencyResolver dependencyResolver;

    @Setup(Level.Invocation)
    public void init() throws BigDataTestFrameworkException {
        flumeUnitContainer = new FlumeUnitContainer();
        dependencyResolver = new DependencyResolver();
        dependencyResolver.resolve(flumeUnitContainer);
    }

    @Benchmark
    public void measureHdfsStart() throws BigDataTestFrameworkException {
        dependencyResolver.destroy(flumeUnitContainer);
    }

}
