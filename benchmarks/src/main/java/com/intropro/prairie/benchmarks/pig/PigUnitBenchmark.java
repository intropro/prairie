package com.intropro.prairie.benchmarks.pig;

import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.PrairieException;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.pig.PigUnit;
import org.apache.commons.io.IOUtils;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
public class PigUnitBenchmark {

    @PrairieUnit
    private PigUnit pigUnit;

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    private DependencyResolver dependencyResolver;

    private String script;
    private Map<String, String> placeholders;

    @Setup(Level.Invocation)
    public void init() throws PrairieException, IOException {
        dependencyResolver = new DependencyResolver();
        dependencyResolver.resolve(this);
        hdfsUnit.saveAs(PigUnitBenchmark.class.getClassLoader().getResourceAsStream("pig/input.csv"), "/data/input/part-00000",
                new TextFormat(), new TextFormat());
        script = IOUtils.toString(PigUnitBenchmark.class.getClassLoader().getResourceAsStream("pig/test.pig"));
        placeholders = new HashMap<>();
        placeholders.put("INPUT_PATH", "/data/input");
        placeholders.put("OUTPUT_PATH", "/data/output");
    }

    @Benchmark
    public void measureStop() throws PrairieException, IOException {
        pigUnit.run(script, placeholders);
    }

    @TearDown(Level.Invocation)
    public void destroy() throws DestroyUnitException {
        dependencyResolver.destroy(this);
    }

}
