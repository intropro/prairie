package com.intropro.prairie.benchmarks.hive2;

import com.intropro.prairie.format.seq.SequenceFormat;
import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.unit.common.exception.BigDataTestFrameworkException;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.hive2.Hive2Unit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 12/10/15.
 */
@Fork(3)
@Warmup(iterations = 0)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SampleTime)
@State(Scope.Thread)
@Threads(1)
public class Hive2ExternalTableBenchmark {

    @BigDataUnit
    private Hive2Unit hive2Unit;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    private DependencyResolver dependencyResolver;

    private Map<String, String> placeholders;

    @Setup(Level.Invocation)
    public void init() throws BigDataTestFrameworkException, SQLException, IOException {
        dependencyResolver = new DependencyResolver();
        dependencyResolver.resolve(this);
        String table1Location = "/data/test_table1";
        String table2Location = "/data/test_table2";
        Path dataDirPath = new Path("/data");
        hdfsUnit.getFileSystem().mkdirs(dataDirPath);
        hdfsUnit.getFileSystem().setOwner(dataDirPath, "hive", "hive");
        hdfsUnit.saveAs(Hive2ExternalTableBenchmark.class.getClassLoader().getResourceAsStream("hive/external/test_table_1.csv"),
                table1Location + "/part-00000", new TextFormat(), new SequenceFormat());
        hdfsUnit.saveAs(Hive2ExternalTableBenchmark.class.getClassLoader().getResourceAsStream("hive/external/test_table_2.csv"),
                table2Location + "/part-00000", new TextFormat(), new SequenceFormat());
        placeholders = new HashMap<>();
        placeholders.put("TEST_TABLE1_LOC", table1Location);
        placeholders.put("TEST_TABLE2_LOC", table2Location);
        placeholders.put("TEST_TABLE1_2_LOC", "/data/test_table1_2");
    }

    @Benchmark
    public void measureExternalTable() throws Exception {
        String query = IOUtils.toString(Hive2ExternalTableBenchmark.class.getClassLoader().getResourceAsStream("hive/external/external_table.hql"));
        hive2Unit.execute(query, placeholders);
    }

    @TearDown(Level.Invocation)
    public void destroy() throws DestroyUnitException {
        dependencyResolver.destroy(this);
    }

}
