package com.intropro.prairie.unit.hdfs;

import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.InputStream;

/**
 * Created by presidentio on 03.09.15.
 */
@RunWith(BigDataTestRunner.class)
public class HdfsUnitTest {

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    @Test
    public void testWriteRead() throws Exception {
        FileSystem fileSystem = hdfsUnit.getFileSystem();
        Path file1 = new Path("/test-file-1");
        String fileContentExpected = "Data for test file 1";
        FSDataOutputStream fsDataOutputStream = fileSystem.create(file1);
        fsDataOutputStream.writeUTF(fileContentExpected);
        fsDataOutputStream.close();
        FSDataInputStream fsDataInputStream = fileSystem.open(file1);
        String fileContent = fsDataInputStream.readUTF();
        Assert.assertEquals(fileContentExpected, fileContent);
    }

    @Test
    public void testSaveAs() throws Exception {
        String dspPath = "/saveas.dat";
        String resourcePath = "saveas/saveas.dat";
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath),
                dspPath, new TextFormat(), new TextFormat());
        hdfsUnit.compare(new Path(dspPath), new TextFormat(), resourcePath, new TextFormat()).assertEquals();
    }

    @Test
    public void testCompareWithFile() throws Exception {
        String dspPath1 = "/comparewithfile1.dat";
        String dspPath2 = "/comparewithfile2.dat";
        String resourcePath = "comparewithfile/comparewithfile1.dat";
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath),
                dspPath1, new TextFormat(), new TextFormat());
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath),
                dspPath2, new TextFormat(), new TextFormat());
        hdfsUnit.compare(new Path(dspPath1), new TextFormat(), new Path(dspPath2), new TextFormat())
                .assertEquals();
    }

    @Test(expected = AssertionError.class)
    public void testCompareWithFileNegative() throws Exception {
        String dspPath1 = "/comparewithfile1.dat";
        String dspPath2 = "/comparewithfile2.dat";
        String resourcePath1 = "comparewithfile/comparewithfile1.dat";
        String resourcePath2 = "comparewithfile/comparewithfile2.dat";
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath1),
                dspPath1, new TextFormat(), new TextFormat());
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath2),
                dspPath2, new TextFormat(), new TextFormat());
        hdfsUnit.compare(new Path(dspPath1), new TextFormat(), new Path(dspPath2), new TextFormat())
                .assertEquals();
    }

    @Test
    public void testCompareWithResource() throws Exception {
        String dspPath1 = "/comparewithfile1.dat";
        String resourcePath = "comparewithfile/comparewithfile1.dat";
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath),
                dspPath1, new TextFormat(), new TextFormat());
        hdfsUnit.compare(new Path(dspPath1), new TextFormat(), resourcePath, new TextFormat())
                .assertEquals();
    }

    @Test(expected = AssertionError.class)
    public void testCompareWithResourceNegative() throws Exception {
        String dspPath1 = "/comparewithfile1.dat";
        String resourcePath1 = "comparewithfile/comparewithfile1.dat";
        String resourcePath2 = "comparewithfile/comparewithfile2.dat";
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath1),
                dspPath1, new TextFormat(), new TextFormat());
        hdfsUnit.compare(new Path(dspPath1), new TextFormat(), resourcePath2, new TextFormat())
                .assertEquals();
    }

    @Test
    public void testCompareWithStream() throws Exception {
        String dspPath1 = "/comparewithfile1.dat";
        String resourcePath = "comparewithfile/comparewithfile1.dat";
        InputStream inputStream = HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath);
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath),
                dspPath1, new TextFormat(), new TextFormat());
        hdfsUnit.compare(new Path(dspPath1), new TextFormat(), inputStream, new TextFormat())
                .assertEquals();
    }

    @Test(expected = AssertionError.class)
    public void testCompareWithStreamNegative() throws Exception {
        String dspPath1 = "/comparewithfile1.dat";
        String resourcePath1 = "comparewithfile/comparewithfile1.dat";
        String resourcePath2 = "comparewithfile/comparewithfile2.dat";
        InputStream inputStream = HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath2);
        hdfsUnit.saveAs(HdfsUnitTest.class.getClassLoader().getResourceAsStream(resourcePath1),
                dspPath1, new TextFormat(), new TextFormat());
        hdfsUnit.compare(new Path(dspPath1), new TextFormat(), inputStream, new TextFormat())
                .assertEquals();
    }
}