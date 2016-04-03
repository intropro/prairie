package com.intropro.prairie.unit.hbase;

import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by presidentio on 4/2/16.
 */
@RunWith(PrairieRunner.class)
public class HBaseUnitTest {

    private String tableName = "test-table";
    private String familyName = "test-family";
    private String columnName = "test-column";

    @PrairieUnit
    private HBaseUnit hBaseUnit;

    private Connection connection;

    @Before
    public void setUp() throws Exception {
        connection = ConnectionFactory.createConnection(hBaseUnit.getConfig());
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void testCreateTable() throws Exception {
        Admin admin = connection.getAdmin();

        TableName[] tableNames = admin.listTableNames();
        Assert.assertEquals(0, tableNames.length);

        createTestTable(connection);

        tableNames = admin.listTableNames();
        Assert.assertEquals(1, tableNames.length);
        Assert.assertEquals(tableNames[0].getNameAsString(), tableName);
    }

    @Test
    public void testInsertGetDelete() throws Exception {
        createTestTable(connection);

        Table table = connection.getTable(TableName.valueOf(tableName));
        String rowId = "test-row-id";
        String rowValue = "test-row-value";
        Put put = new Put(Bytes.toBytes(rowId));
        put.add(new KeyValue(Bytes.toBytes(rowId), Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(rowValue)));
        table.put(put);

        Result result = table.get(new Get(Bytes.toBytes(rowId)));
        Assert.assertEquals(rowValue,
                Bytes.toString(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName))));

        table.delete(new Delete(Bytes.toBytes(rowId)));
        result = table.get(new Get(Bytes.toBytes(rowId)));
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testScan() throws Exception {
        createTestTable(connection);

        int rowCount = 100;
        Table table = connection.getTable(TableName.valueOf(tableName));
        String rowId = "test-row-id";
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            Put put = new Put(Bytes.toBytes(rowId + formatNumber(i)));
            put.add(new KeyValue(Bytes.toBytes(rowId + formatNumber(i)), Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(i)));
            puts.add(put);
        }
        table.put(puts);

        int start = 55;
        int end = 80;
        Scan scan = new Scan(Bytes.toBytes(rowId + formatNumber(start)), Bytes.toBytes(rowId + formatNumber(end)));
        scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        ResultScanner resultScanner = table.getScanner(scan);
        int resultCount = 0;
        for (Result result : resultScanner) {
            int value = Bytes.toInt(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)));
            Assert.assertTrue("Should be >= " + start + ", but " + value,value >= start);
            Assert.assertTrue("Should be <= " + end + ", but " + value, value <= end);
            resultCount++;
        }
        Assert.assertEquals(end - start, resultCount);
    }

    private String formatNumber(int number){
        return java.lang.String.format("%03d", number);
    }

    private void createTestTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
    }
}