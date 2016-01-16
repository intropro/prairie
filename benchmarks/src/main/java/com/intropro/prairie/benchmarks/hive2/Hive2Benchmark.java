package com.intropro.prairie.benchmarks.hive2;

import com.intropro.prairie.unit.common.DependencyResolver;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.PrairieException;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.hive2.Hive2Unit;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 12/10/15.
 */
@Fork(2)
@Warmup(iterations = 0)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.SampleTime)
@State(Scope.Thread)
@Threads(1)
public class Hive2Benchmark {

    @PrairieUnit
    private Hive2Unit hive2Unit;

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    private DependencyResolver dependencyResolver;

    @Setup(Level.Invocation)
    public void init() throws PrairieException, SQLException, IOException {
        dependencyResolver = new DependencyResolver();
        dependencyResolver.resolve(this);
        String createTableScript = "create table test_table (id bigint, field2 string)";
        hive2Unit.createClient().execute(createTableScript);
        String insertScript = "insert into table test_table values (1, '2'), (3, '4')";
        hive2Unit.createClient().execute(insertScript);
    }

    @Benchmark
    public void measureCreateTable() throws Exception {
        hive2Unit.createClient().execute("create table table1 (id bigint, field1 string)");
    }

    @Benchmark
    public void measureShowTables() throws Exception {
        hive2Unit.createClient().executeQuery("show tables");
    }

    @Benchmark
    public void measureInsert() throws Exception {
        String insertScript = "insert into table test_table values (1, '2'), (3, '4')";
        hive2Unit.createClient().execute(insertScript);
    }

    @Benchmark
    public void measureSelect() throws Exception {
        String selectScript = "select * from test_table";
        hive2Unit.createClient().executeQuery(selectScript);
    }

    @TearDown(Level.Invocation)
    public void destroy() throws DestroyUnitException {
        dependencyResolver.destroy(this);
    }

}
