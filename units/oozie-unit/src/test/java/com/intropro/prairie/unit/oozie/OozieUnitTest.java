package com.intropro.prairie.unit.oozie;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.CompareResponse;
import com.intropro.prairie.comparator.EntryComparator;
import com.intropro.prairie.format.sv.SvFormat;
import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.cmd.CmdUnit;
import com.intropro.prairie.unit.common.Version;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.hive2.Hive2Unit;
import com.intropro.prairie.unit.yarn.YarnUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
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
@RunWith(PrairieRunner.class)
public class OozieUnitTest {

    @PrairieUnit
    private OozieUnit oozieUnit;

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @PrairieUnit
    private YarnUnit yarnUnit;

    @PrairieUnit
    private Hive2Unit hiveUnit;

    @PrairieUnit
    private CmdUnit cmdUnit;

    private EntryComparator<String> byLineComparator = new ByLineComparator<>();

    private String workflowPath;

    private SaveArgsCommand command;

    @Test
    public void testOozie() throws Exception {
        Assume.assumeTrue(Hive2Unit.VERSION.compareTo(new Version("0.14.0")) >= 0);
        Properties properties = new Properties();
        properties.load(OozieUnitTest.class.getClassLoader().getResourceAsStream("job.properties"));
        properties.setProperty("nameNode", hdfsUnit.getNamenode());
        properties.setProperty("jobTracker", yarnUnit.getJobTracker());
        workflowPath = properties.getProperty("appPath");

        deployWorkflow(properties);
        prepareJavaAction(properties);
        prepareMapredAction(properties);
        prepareHiveAction(properties);
        preparePigAction(properties);
        prepareShellAction(properties);

        OozieJob oozieJob = oozieUnit.run(properties);
        oozieJob.waitFinish(TimeUnit.MINUTES.toMillis(15));
        Assert.assertEquals(WorkflowJob.Status.SUCCEEDED, oozieJob.getWorkflowJob().getStatus());

        checkJavaAction(properties);
        checkMapredAction(properties);
        checkHive2Action(properties);
        checkPigAction(properties);
        checkShellAction(properties);
    }

    private void deployWorkflow(Properties properties) throws IOException {
        hdfsUnit.getFileSystem().mkdirs(new Path(workflowPath));
        FSDataOutputStream dataOutputStream =
                hdfsUnit.getFileSystem().create(new Path(workflowPath, "workflow.xml"));
        IOUtils.copy(OozieUnitTest.class.getClassLoader().getResourceAsStream("workflow.xml"), dataOutputStream);
        dataOutputStream.close();
    }

    private void prepareJavaAction(Properties properties) throws IOException {
        File file = File.createTempFile("prairie-oozie-test", ".out");
        file.deleteOnExit();
        properties.setProperty("outFile", file.getAbsolutePath());
    }

    private void prepareMapredAction(Properties properties) throws IOException {
        String mapredInput = properties.getProperty("mapredInput");
        String mapredOutput = properties.getProperty("mapredOutput");
        hdfsUnit.getFileSystem().mkdirs(new Path(mapredInput));
        FSDataOutputStream fsDataOutputStream = hdfsUnit.getFileSystem().create(new Path(mapredInput, "input"));
        IOUtils.copy(OozieUnitTest.class.getClassLoader().getResourceAsStream("mapred-action/input"), fsDataOutputStream);
        fsDataOutputStream.close();
        hdfsUnit.getFileSystem().mkdirs(new Path(mapredOutput).getParent());
        hdfsUnit.getFileSystem().setOwner(new Path(mapredOutput).getParent(), "oozie", "oozie");
    }

    private void prepareHiveAction(Properties properties) throws IOException, SQLException {
        FSDataOutputStream scriptOutputStream =
                hdfsUnit.getFileSystem().create(new Path(workflowPath, "hive2-action/hive-action-query.hql"));
        InputStream scriptInputStream;
        if (Hive2Unit.VERSION.compareTo(new Version("1.1.1")) <= 0 && HdfsUnit.VERSION.compareTo(new Version("2.5.2")) <= 0) {
            scriptInputStream = OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/hive-action-query-hive-1.1.1.hql");
        } else {
            scriptInputStream = OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/hive-action-query.hql");
        }
        IOUtils.copy(scriptInputStream, scriptOutputStream);
        scriptInputStream.close();
        scriptOutputStream.close();

        Path hiveDataDir = new Path("/data/hive");
        hdfsUnit.getFileSystem().mkdirs(hiveDataDir);

        Path testTable1Loc = new Path(hiveDataDir, "table1");
        hdfsUnit.saveAs(OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/test_table1.csv"),
                new Path(testTable1Loc, "part-00000").toString(), new TextFormat(), new TextFormat());

        HiveConf hiveConf = hiveUnit.getConfig();
        hiveConf.unset("mapreduce.jobtracker.address");
        hiveConf.unset("yarn.resourcemanager.address");
        hiveConf.unset("fs.default.name");
        Path hiveDefaultPath = new Path(workflowPath, "conf/hive-site.xml");
        FSDataOutputStream hiveConfOutStr = hdfsUnit.getFileSystem().create(hiveDefaultPath);
        hiveConf.writeXml(hiveConfOutStr);
        hiveConfOutStr.close();
        properties.setProperty("HIVE_DEFAULTS", hiveDefaultPath.toString());
        properties.setProperty("HIVE_JDBC_URL", hiveUnit.getJdbcUrl());
        properties.setProperty("TEST_TABLE1_LOC", testTable1Loc.toString());
    }

    private void preparePigAction(Properties properties) throws IOException {
        hdfsUnit.getFileSystem().mkdirs(new Path(properties.getProperty("pigOutput")).getParent());
        hdfsUnit.getFileSystem().setOwner(new Path(properties.getProperty("pigOutput")).getParent(), "oozie", "oozie");
        hdfsUnit.saveAs(OozieUnitTest.class.getClassLoader().getResourceAsStream("pig-action/pig-action.pig"),
                new Path(workflowPath, "pig-action.pig").toString(), new TextFormat(), new TextFormat());
        hdfsUnit.saveAs(OozieUnitTest.class.getClassLoader().getResourceAsStream("pig-action/input.csv"),
                properties.getProperty("pigInput"), new TextFormat(), new TextFormat());
        properties.setProperty("INPUT_PATH", hdfsUnit.getNamenode() + properties.getProperty("pigInput"));
        properties.setProperty("OUTPUT_PATH", hdfsUnit.getNamenode() + properties.getProperty("pigOutput"));
    }

    private void prepareShellAction(Properties properties) throws IOException {
        command = new SaveArgsCommand();
        cmdUnit.declare("test-command", command);

    }

    private void checkJavaAction(Properties properties) throws IOException {
        Assert.assertEquals("Java; Unexpected result", properties.getProperty("text"),
                IOUtils.toString(new FileReader(properties.getProperty("outFile"))));
    }

    private void checkMapredAction(Properties properties) throws IOException {
        String mapredOutput = properties.getProperty("mapredOutput");
        List<String> exprectedMapredOutput = IOUtils.readLines(
                OozieUnitTest.class.getClassLoader().getResourceAsStream("mapred-action/output"));
        List<String> resultMapredOutput = new ArrayList<>();
        for (FileStatus fileStatus : hdfsUnit.getFileSystem().listStatus(new Path(mapredOutput))) {
            FSDataInputStream mapredInputStream = hdfsUnit.getFileSystem().open(fileStatus.getPath());
            resultMapredOutput.addAll(IOUtils.readLines(mapredInputStream));
            mapredInputStream.close();
        }
        CompareResponse compareResponse = byLineComparator.compare(exprectedMapredOutput, resultMapredOutput);
        Assert.assertTrue("Mapred; Unexpected lines: " + compareResponse.getUnexpected(),
                compareResponse.getUnexpected().isEmpty());
        Assert.assertTrue("Mapred; Missed lines: " + compareResponse.getMissed(),
                compareResponse.getMissed().isEmpty());
    }

    private void checkHive2Action(Properties properties) throws SQLException, IOException {
        if (Hive2Unit.VERSION.compareTo(new Version("1.1.1")) <= 0 && HdfsUnit.VERSION.compareTo(new Version("2.5.2")) <= 0) {
            List<Map<String, String>> tables = hiveUnit.createClient().executeQuery("show tables;");
            Assert.assertEquals(1, tables.size());
            Assert.assertEquals("test_table1", tables.get(0).get("tab_name"));
        } else {
            hiveUnit.createClient().compare("select * from test_table1",
                    OozieUnitTest.class.getClassLoader().getResourceAsStream("hive2-action/output.csv"),
                    new SvFormat('|')).assertEquals();
        }
    }

    private void checkPigAction(Properties properties) throws IOException {
        hdfsUnit.compare(new Path(properties.getProperty("pigOutput")), new TextFormat(),
                "pig-action/output.csv", new TextFormat());
    }

    private void checkShellAction(Properties properties) {
        Assert.assertNotNull(command.getArgs());
        Assert.assertEquals(2, command.getArgs().size());
        Assert.assertTrue(command.getArgs().contains("firstArgument"));
        Assert.assertTrue(command.getArgs().contains("secondArgument"));
    }

}