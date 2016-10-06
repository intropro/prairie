package com.intropro.prairie.unit.sshd;

import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.kerberos.KerberosUnit;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 8/25/16.
 */
@RunWith(PrairieRunner.class)
public class SshdUnitTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @PrairieUnit
    private SshdUnit sshdUnit;

    @PrairieUnit
    private KerberosUnit kerberosUnit;

    @Test
    public void testSsh() throws Exception {
        File outputFile = folder.newFile("output");
        folder.newFile("f1").createNewFile();
        folder.newFile("f2").createNewFile();
        folder.newFile("f3").createNewFile();
        ProcessBuilder processBuilder = new ProcessBuilder("ssh", "-p", "" + sshdUnit.getPort(),
                "-o", "StrictHostKeyChecking=no",
                "-i", sshdUnit.getPrivateKeyPath().toAbsolutePath().toString(),
                sshdUnit.getDefaultUsername() + "@" + sshdUnit.getHost());
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.to(outputFile));
        Process process = processBuilder.start();
        process.getOutputStream().write(("cd " + folder.getRoot().getAbsolutePath() + "\n").getBytes());
        process.getOutputStream().write("ls".getBytes());
        process.getOutputStream().close();
        process.waitFor();
        String expected = IOUtils.toString(SshdUnitTest.class.getResourceAsStream("/ls/expected.dat"));
        String result = IOUtils.toString(new FileInputStream(outputFile));
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testScp() throws Exception {
        File outputFile = folder.newFile("output");
        File inputFile = folder.newFile("input");
        FileOutputStream fileOutputStream = new FileOutputStream(inputFile);
        IOUtils.copy(SshdUnitTest.class.getResourceAsStream("/scp/input.dat"), fileOutputStream);
        fileOutputStream.close();
        ProcessBuilder processBuilder = new ProcessBuilder("scp", "-P", "" + sshdUnit.getPort(),
                "-o", "StrictHostKeyChecking=no",
                "-i", sshdUnit.getPrivateKeyPath().toAbsolutePath().toString(),
                sshdUnit.getDefaultUsername() + "@" + sshdUnit.getHost() + ":" + inputFile.getAbsolutePath(),
                outputFile.getAbsolutePath());
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        Process process = processBuilder.start();
        process.waitFor();
        String expected = IOUtils.toString(SshdUnitTest.class.getResourceAsStream("/scp/input.dat"));
        String result = IOUtils.toString(new FileInputStream(outputFile));
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testScpWithKerberos() throws Exception {
        kerberosUnit.getKerberosUserManager().addUser("presidentio", "pass");
        sshdUnit.addKerberosUser("presidentio");
        Thread.sleep(TimeUnit.MINUTES.toMillis(100));
        File outputFile = folder.newFile("output");
        File inputFile = folder.newFile("input");
        FileOutputStream fileOutputStream = new FileOutputStream(inputFile);
        IOUtils.copy(SshdUnitTest.class.getResourceAsStream("/scp/input.dat"), fileOutputStream);
        fileOutputStream.close();
        ProcessBuilder processBuilder = new ProcessBuilder("scp", "-P", "" + sshdUnit.getPort(),
                "-o", "StrictHostKeyChecking=no",
                "-i", sshdUnit.getPrivateKeyPath().toAbsolutePath().toString(),
                sshdUnit.getDefaultUsername() + "@" + sshdUnit.getHost() + ":" + inputFile.getAbsolutePath(),
                outputFile.getAbsolutePath());
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        Process process = processBuilder.start();
        process.waitFor();
        String expected = IOUtils.toString(SshdUnitTest.class.getResourceAsStream("/scp/input.dat"));
        String result = IOUtils.toString(new FileInputStream(outputFile));
        Assert.assertEquals(expected, result);
    }
}