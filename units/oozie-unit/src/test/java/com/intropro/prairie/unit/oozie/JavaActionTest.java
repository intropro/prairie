package com.intropro.prairie.unit.oozie;

import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.yarn.YarnUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 9/14/15.
 */
@RunWith(PrairieRunner.class)
public class JavaActionTest {

    @PrairieUnit
    private OozieUnit oozieUnit;

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @PrairieUnit
    private YarnUnit yarnUnit;

    private String workflowPath;

    @Test
    public void testOozie() throws Exception {
        Properties properties = new Properties();
        properties.load(JavaActionTest.class.getClassLoader().getResourceAsStream("java-action-test/job.properties"));
        properties.setProperty("nameNode", hdfsUnit.getNamenode());
        properties.setProperty("jobTracker", yarnUnit.getJobTracker());
        workflowPath = properties.getProperty("appPath");

        deployWorkflow(properties);
        prepareJavaAction(properties);

        OozieJob oozieJob = oozieUnit.run(properties);
        oozieJob.waitFinish(TimeUnit.MINUTES.toMillis(15));
        Assert.assertEquals(WorkflowJob.Status.SUCCEEDED, oozieJob.getWorkflowJob().getStatus());

        checkJavaAction(properties);
    }

    private void deployWorkflow(Properties properties) throws IOException {
        hdfsUnit.getFileSystem().mkdirs(new Path(workflowPath));
        FSDataOutputStream dataOutputStream =
                hdfsUnit.getFileSystem().create(new Path(workflowPath, "workflow.xml"));
        IOUtils.copy(JavaActionTest.class.getClassLoader().getResourceAsStream("java-action-test/workflow.xml"), dataOutputStream);
        dataOutputStream.close();
    }

    private void prepareJavaAction(Properties properties) throws IOException {
        File file = File.createTempFile("prairie-oozie-test", ".out");
        file.deleteOnExit();
        properties.setProperty("outFile", file.getAbsolutePath());
    }

    private void checkJavaAction(Properties properties) throws IOException {
        Assert.assertEquals("Java; Unexpected result", properties.getProperty("text"),
                IOUtils.toString(new FileReader(properties.getProperty("outFile"))));
    }

}