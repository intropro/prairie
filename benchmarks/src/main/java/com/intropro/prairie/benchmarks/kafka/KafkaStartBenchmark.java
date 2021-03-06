package com.intropro.prairie.benchmarks.kafka;

import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.exception.PrairieException;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
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
public class KafkaStartBenchmark {

    private KafkaUnitContainer kafkaUnitContainer;
    private DependencyResolver dependencyResolver;

    @Setup(Level.Invocation)
    public void init() {
        kafkaUnitContainer = new KafkaUnitContainer();
        dependencyResolver = new DependencyResolver();
    }

    @Benchmark
    public void measureStart() throws PrairieException {
        dependencyResolver.resolve(kafkaUnitContainer);
    }

    @TearDown(Level.Invocation)
    public void destroy() throws DestroyUnitException {
        dependencyResolver.destroy(kafkaUnitContainer);
    }
}
