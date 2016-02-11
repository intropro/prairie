package com.intropro.prairie.unit.hive2;

import com.intropro.prairie.format.sv.SvFormat;
import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.Version;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by presidentio on 07.09.15.
 */
@RunWith(PrairieRunner.class)
public class Hive2UnitTest {

    @PrairieUnit
    private static Hive2Unit hive2Unit;

    @PrairieUnit
    private static HdfsUnit hdfsUnit;

    @Test
    public void testForDemo() throws Exception {
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("0.13.1")) > 0);
        //Due to HIVE-9957
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("1.1.1")) != 0);
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testForDemo; use testForDemo;");
        client.execute("use testForDemo; create table prairie_test_table (id bigint, name string)");
        client.execute("use testForDemo; insert into table prairie_test_table values (1, 'first')");
        List<Map<String, String>> tableContent = client.executeQuery("use testForDemo; select * from prairie_test_table");
        Map<String, String> expectedRaw = new HashMap<>();
        expectedRaw.put("prairie_test_table.id", "1");
        expectedRaw.put("prairie_test_table.name", "first");
        Assert.assertEquals(expectedRaw, tableContent.get(0));
    }

    @Test
    public void testCreateTable() throws Exception {
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testCreateTable; use testCreateTable;");
        String tableName1 = "test_table_1";
        String tableName2 = "test_table_2";
        String createTableScript = "create table %s (id bigint, field1 string)";
        client.execute(String.format(createTableScript, tableName1));
        client.execute(String.format(createTableScript, tableName2));
        List<Map<String, String>> tables = client.executeQuery("show tables");
        Assert.assertEquals(2, tables.size());
        Assert.assertEquals(1, tables.get(0).size());
        String tableNameColumn = "tab_name";
        List<String> tableNames = Arrays.asList(tables.get(0).get(tableNameColumn), tables.get(1).get(tableNameColumn));
        Assert.assertTrue(tableNames.contains(tableName1));
        Assert.assertTrue(tableNames.contains(tableName2));
    }

    @Test
    public void testInsertSelect() throws Exception {
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("0.13.1")) > 0);
        //HIVE-9957
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("1.1.1")) > 0 || HdfsUnit.VERSION.compareTo(new Version("2.5.2")) > 0);
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testInsertSelect; use testInsertSelect;");
        String createTableScript = "create table test_table (id bigint, field2 string)";
        String insertScript = "insert into table test_table values (%s)";
        String selectScript = "select * from test_table";
        client.execute(createTableScript);
        List<String> csvLines = IOUtils.readLines(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/data/test_table.csv"));
        csvLines.remove(0);
        for (String csvLine : csvLines) {
            client.execute(String.format(insertScript, csvLine));
        }
        client.compare(selectScript, "hive/data/test_table.csv", new SvFormat(',')).assertEquals();
    }

    @Test
    public void testExternalTable() throws Exception {
        //HIVE-9957
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("1.1.1")) > 0 || HdfsUnit.VERSION.compareTo(new Version("2.5.2")) > 0);
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testExternalTable; use testExternalTable;");
        String table1Location = "/data/test_table1";
        Path dataDirPath = new Path("/data");
        hdfsUnit.getFileSystem().mkdirs(dataDirPath);
        hdfsUnit.getFileSystem().setOwner(dataDirPath, "hive", "hive");
        hdfsUnit.saveAs(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/external/test_table_1.csv"),
                table1Location + "/part-00000", new TextFormat(), new TextFormat());
        String query = IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/external/external_table.hql"));
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("TEST_TABLE1_LOC", table1Location);
        client.execute(query, placeholders);
        client.compare("SELECT * FROM test_table1;", "hive/external/expected.csv", new SvFormat('|')).assertEquals();
    }

    @Test
    public void testJoin() throws Exception {
        //Skipped for 1.1.1 due to HIVE-11249
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("1.1.1")) != 0);
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testJoin; use testJoin;");
        String table1Location = "/data/test_table1";
        String table2Location = "/data/test_table2";
        Path dataDirPath = new Path("/data");
        hdfsUnit.getFileSystem().mkdirs(dataDirPath);
        hdfsUnit.getFileSystem().setOwner(dataDirPath, "hive", "hive");
        hdfsUnit.saveAs(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/join/test_table_1.csv"),
                table1Location + "/part-00000", new TextFormat(), new TextFormat());
        hdfsUnit.saveAs(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/join/test_table_2.csv"),
                table2Location + "/part-00000", new TextFormat(), new TextFormat());
        String query = IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/join/join.hql"));
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("TEST_TABLE1_LOC", table1Location);
        placeholders.put("TEST_TABLE2_LOC", table2Location);
        placeholders.put("TEST_TABLE1_2_LOC", "/data/test_table1_2");
        client.execute(query, placeholders);
        client.compare("SELECT * FROM test_table1_2;", "hive/join/test_table.csv", new SvFormat(',')).assertEquals();
    }

    @Test
    public void testSet() throws Exception {
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testSet; use testSet;");
        client.execute(IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/set/set.hql")));
        client.compare("show tables", "hive/set/expected.csv", new SvFormat(',')).assertEquals();
    }

    @Test
    public void testCommentSuccess() throws Exception {
        //HIVE-9957
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("1.1.1")) > 0 || HdfsUnit.VERSION.compareTo(new Version("2.5.2")) > 0);
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testCommentSuccess; use testCommentSuccess;");
        client.execute(IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/comment/comment-success.hql")));
        client.compare("show tables", "hive/comment/expected.csv", new SvFormat(',')).assertEquals();
    }

    @Test(expected = SQLException.class)
    public void testCommentFailed() throws Exception {
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testCommentFailed; use testCommentFailed;");
        client.execute(IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/comment/comment-failed.hql")));
    }

    @Test
    public void testSequence() throws Exception {
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("1.1.1")) != 0);
        Hive2UnitClient client = hive2Unit.createClient();
        client.execute("create database testSequence; use testSequence;");
        client.execute(IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/sequence/create-sequence.hql")));
        client.compare("show tables", "hive/sequence/expected.csv", new SvFormat(',')).assertEquals();
    }
}