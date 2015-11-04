package com.intropro.prairie.unit.oozie;

import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.CompareResponse;
import com.intropro.prairie.comparator.EntryComparator;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.format.InputFormatReader;
import com.intropro.prairie.format.seq.SequenceFormat;
import com.intropro.prairie.format.sv.SvFormat;
import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.unit.hive2.Hive2Unit;
import com.intropro.prairie.unit.yarn.YarnUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 9/14/15.
 */
@RunWith(BigDataTestRunner.class)
public class OozieUnitTest {

    @BigDataUnit
    private OozieUnit oozieUnit;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    @BigDataUnit
    private YarnUnit yarnUnit;

    @BigDataUnit
    private Hive2Unit hiveUnit;

    private EntryComparator<String> byLineComparator = new ByLineComparator<>();
    private EntryComparator<Map<String, String>> mapComparator = new ByLineComparator<>();

    private String workflowPath;

    private String mapredInput;
    private String mapredOutput;

    private String hiveOutput;

    @Test
    public void testOozie() throws Exception {
        Properties properties = new Properties();
        properties.load(OozieUnitTest.class.getClassLoader().getResourceAsStream("job.properties"));
        properties.setProperty("nameNode", hdfsUnit.getFileSystem().getUri().toString());
        properties.setProperty("jobTracker", yarnUnit.getConfig().get(YarnConfiguration.RM_ADDRESS));
        workflowPath = properties.getProperty("appPath");
        mapredInput = properties.getProperty("mapredInput");
        mapredOutput = properties.getProperty("mapredOutput");
        hiveOutput = properties.getProperty("TEST_TABLE1_2_LOC");
        deployWorkflow();
        prepareMapredAction();
        properties.putAll(prepareHiveAction());
        OozieJob oozieJob = oozieUnit.run(properties);
        oozieJob.waitFinish(TimeUnit.MINUTES.toMillis(5));
        Assert.assertEquals(WorkflowJob.Status.SUCCEEDED, oozieJob.getWorkflowJob().getStatus());
        //Check java action
        Assert.assertEquals("Java; Unexpected result", properties.getProperty("text"), IOUtils.toString(new FileReader(properties.getProperty("outFile"))));
        //Check mapred action
        List<String> exprectedMapredOutput = IOUtils.readLines(OozieUnitTest.class.getClassLoader().getResourceAsStream("mapred-action/output"));
        List<String> resultMapredOutput = new ArrayList<>();
        for (FileStatus fileStatus : hdfsUnit.getFileSystem().listStatus(new Path(mapredOutput))) {
            FSDataInputStream mapredInputStream = hdfsUnit.getFileSystem().open(fileStatus.getPath());
            resultMapredOutput.addAll(IOUtils.readLines(mapredInputStream));
            mapredInputStream.close();
        }
        CompareResponse compareResponse = byLineComparator.compare(exprectedMapredOutput, resultMapredOutput);
        Assert.assertTrue("Mapred; Unexpected lines: " + compareResponse.getUnexpected(), compareResponse.getUnexpected().isEmpty());
        Assert.assertTrue("Mapred; Missed lines: " + compareResponse.getMissed(), compareResponse.getMissed().isEmpty());
        //Check hive action
        List<Map<String, String>> hiveActionResult = hiveUnit.executeQuery("select * from test_table1_2");
        InputFormatReader<Map<String, String>> reader = new SvFormat(',').createReader(OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/output.csv"));
        List<Map<String, String>> exprectedHiveOutput = reader.all();
        reader.close();
        CompareResponse hiveCompareResponse = mapComparator.compare(exprectedHiveOutput, hiveActionResult);
        Assert.assertTrue("Hive; Unexpected lines: " + hiveCompareResponse.getUnexpected(), hiveCompareResponse.getUnexpected().isEmpty());
        Assert.assertTrue("Hive; Missed lines: " + hiveCompareResponse.getMissed(), hiveCompareResponse.getMissed().isEmpty());
    }

    private void prepareMapredAction() throws IOException {
        hdfsUnit.getFileSystem().mkdirs(new Path(mapredInput));
        FSDataOutputStream fsDataOutputStream = hdfsUnit.getFileSystem().create(new Path(mapredInput, "input"));
        IOUtils.copy(OozieUnitTest.class.getClassLoader().getResourceAsStream("mapred-action/input"), fsDataOutputStream);
        fsDataOutputStream.close();
        hdfsUnit.getFileSystem().mkdirs(new Path(mapredOutput).getParent());
        hdfsUnit.getFileSystem().setOwner(new Path(mapredOutput).getParent(), "oozie", "oozie");
    }

    private Properties prepareHiveAction() throws IOException, SQLException {
        FSDataOutputStream scriptOutputStream =
                hdfsUnit.getFileSystem().create(new Path(workflowPath, "hive2-action/hive-action-query.hql"));
        InputStream scriptInputStream = OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/hive-action-query.hql");
        IOUtils.copy(scriptInputStream, scriptOutputStream);
        scriptInputStream.close();
        scriptOutputStream.close();

        Path hiveDataDir = new Path("/data/hive");
        hiveUnit.createDataDir("/data/hive");
        Path outDir = new Path(hiveDataDir, "table1_2");
        hdfsUnit.getFileSystem().mkdirs(outDir);
        hdfsUnit.getFileSystem().setOwner(outDir, "oozie", "oozie");

        Path testTable1Loc = new Path(hiveDataDir, "table1");
        Path testTable2Loc = new Path(hiveDataDir, "table2");

        hdfsUnit.saveAs(OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/test_table1.csv"),
                new Path(testTable1Loc, "part-00000").toString(), new TextFormat(), new SequenceFormat());
        hdfsUnit.saveAs(OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/test_table2.csv"),
                new Path(testTable2Loc, "part-00000").toString(), new TextFormat(), new SequenceFormat());

        HiveConf hiveConf = hiveUnit.getConfig();
        hiveConf.unset("mapreduce.jobtracker.address");
        hiveConf.unset("yarn.resourcemanager.address");
        hiveConf.unset("fs.default.name");
        Path hiveDefaultPath = new Path(workflowPath, "conf/hive-site.xml");
        FSDataOutputStream hiveConfOutStr = hdfsUnit.getFileSystem().create(hiveDefaultPath);
        hiveConf.writeXml(hiveConfOutStr);
        hiveConfOutStr.close();
        Properties properties = new Properties();
        properties.setProperty("HIVE_DEFAULTS", hiveDefaultPath.toString());
        properties.setProperty("HIVE_JDBC_URL", hiveUnit.getJdbcUrl());
        properties.setProperty("TEST_TABLE1_LOC", testTable1Loc.toString());
        properties.setProperty("TEST_TABLE2_LOC", testTable2Loc.toString());
        properties.setProperty("TEST_TABLE1_2_LOC", outDir.toString());
        return properties;
    }

    private void deployWorkflow() throws IOException {
        hdfsUnit.getFileSystem().mkdirs(new Path(workflowPath));
        FSDataOutputStream dataOutputStream =
                hdfsUnit.getFileSystem().create(new Path(workflowPath, "workflow.xml"));
        IOUtils.copy(OozieUnitTest.class.getClassLoader().getResourceAsStream("workflow.xml"), dataOutputStream);
        dataOutputStream.close();
    }

}