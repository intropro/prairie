package com.intropro.prairie.benchmarks.pig;

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
public class PigStopBenchmark {

    private PigUnitContainer pigUnitContainer;
    private DependencyResolver dependencyResolver;

    @Setup(Level.Invocation)
    public void init() throws BigDataTestFrameworkException {
        pigUnitContainer = new PigUnitContainer();
        dependencyResolver = new DependencyResolver();
        dependencyResolver.resolve(pigUnitContainer);
    }

    @Benchmark
    public void measureHdfsStart() throws BigDataTestFrameworkException {
        dependencyResolver.destroy(pigUnitContainer);
    }

}
