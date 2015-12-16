package com.intropro.prairie.unit.hive2;

import com.intropro.prairie.format.seq.SequenceFormat;
import com.intropro.prairie.format.sv.SvFormat;
import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.Version;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
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
@RunWith(BigDataTestRunner.class)
public class Hive2UnitTest {

    @BigDataUnit
    private Hive2Unit hive2Unit;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    @Test
    public void testForDemo() throws Exception {
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("0.13.1")) > 0);
        hive2Unit.execute("create table prairie_test_table (id bigint, name string)");
        hive2Unit.execute("insert into table prairie_test_table values (1, 'first')");
        List<Map<String, String>> tableContent = hive2Unit.executeQuery("select * from prairie_test_table");
        Map<String, String> expectedRaw = new HashMap<>();
        expectedRaw.put("prairie_test_table.id", "1");
        expectedRaw.put("prairie_test_table.name", "first");
        Assert.assertEquals(expectedRaw, tableContent.get(0));
    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName1 = "test_table_1";
        String tableName2 = "test_table_2";
        String createTableScript = "create table %s (id bigint, field1 string)";
        hive2Unit.execute(String.format(createTableScript, tableName1));
        hive2Unit.execute(String.format(createTableScript, tableName2));
        List<Map<String, String>> tables = hive2Unit.executeQuery("show tables");
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
        String createTableScript = "create table test_table (id bigint, field2 string)";
        String insertScript = "insert into table test_table values (%s)";
        String selectScript = "select * from test_table";
        hive2Unit.execute(createTableScript);
        List<String> csvLines = IOUtils.readLines(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/data/test_table.csv"));
        csvLines.remove(0);
        for (String csvLine : csvLines) {
            hive2Unit.execute(String.format(insertScript, csvLine));
        }
        hive2Unit.compare(selectScript, "hive/data/test_table.csv", new SvFormat(',')).assertEquals();
    }

    @Test
    public void testExternalTable() throws Exception {
        String table1Location = "/data/test_table1";
        String table2Location = "/data/test_table2";
        Path dataDirPath = new Path("/data");
        hdfsUnit.getFileSystem().mkdirs(dataDirPath);
        hdfsUnit.getFileSystem().setOwner(dataDirPath, "hive", "hive");
        hdfsUnit.saveAs(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/external/test_table_1.csv"),
                table1Location + "/part-00000", new TextFormat(), new SequenceFormat());
        hdfsUnit.saveAs(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/external/test_table_2.csv"),
                table2Location + "/part-00000", new TextFormat(), new SequenceFormat());
        String query = IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/external/external_table.hql"));
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("TEST_TABLE1_LOC", table1Location);
        placeholders.put("TEST_TABLE2_LOC", table2Location);
        placeholders.put("TEST_TABLE1_2_LOC", "/data/test_table1_2");
        hive2Unit.execute(query, placeholders);
        hive2Unit.compare("SELECT * FROM test_table1_2;", "hive/external/test_table.csv", new SvFormat(',')).assertEquals();
    }

    @Test
    public void testSet() throws Exception {
        hive2Unit.execute(IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/set/set.hql")));
        hive2Unit.compare("show tables", "hive/set/expected.csv", new SvFormat(',')).assertEquals();
    }

    @Test
    public void testCommentSuccess() throws Exception {
        hive2Unit.execute(IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/comment/comment-success.hql")));
        hive2Unit.compare("show tables", "hive/comment/expected.csv", new SvFormat(',')).assertEquals();
    }

    @Test(expected = SQLException.class)
    public void testCommentFailed() throws Exception {
        hive2Unit.execute(IOUtils.toString(Hive2UnitTest.class.getClassLoader().getResourceAsStream("hive/comment/comment-failed.hql")));
    }
}