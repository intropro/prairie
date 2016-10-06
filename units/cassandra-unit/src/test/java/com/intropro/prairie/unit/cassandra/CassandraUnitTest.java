package com.intropro.prairie.unit.cassandra;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * Created by presidentio on 10/4/16.
 */
@RunWith(PrairieRunner.class)
public class CassandraUnitTest {

    @PrairieUnit
    private CassandraUnit cassandraUnit;

    @Test
    public void testCreate() throws Exception {
        String testKeyspace = "prairie_test_keyspace";
        String testTable = "prairie_test_table";
        Session session = cassandraUnit.getClient().getSession();
        session.execute("CREATE KEYSPACE " + testKeyspace
                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE " + testKeyspace + ";");
        session.execute("CREATE TABLE " + testTable + " ( field1 varchar PRIMARY KEY, field2 bigint);");
        List<Row> rows = session.execute("select table_name " +
                "from system_schema.tables where keyspace_name = ?;", testKeyspace).all();
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals(testTable, rows.get(0).getString(0));

    }
}