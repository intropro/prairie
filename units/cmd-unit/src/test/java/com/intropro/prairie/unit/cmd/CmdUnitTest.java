package com.intropro.prairie.unit.cmd;

import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by presidentio on 11/14/15.
 */
@RunWith(BigDataTestRunner.class)
public class CmdUnitTest {

    @BigDataUnit
    private CmdUnit cmdUnit;

    private String alias = "cmd-unit-test";

    private String inputData = "123456asdfg\nzxcvvbn\n123445567";

    private List<String> args = Arrays.asList("1", "2", "a", "asdfqwf", "eg44gg44g");

    @Test
    public void testPipe() throws Exception {
        String path = cmdUnit.declare(alias, new MirrorCommand());
        ProcessBuilder processBuilder = new ProcessBuilder(path);
        Process process = processBuilder.start();
        new StreamRedirect(process.getErrorStream(), System.err).start();
        process.getOutputStream().write(inputData.getBytes());
        process.getOutputStream().close();
        int exitStatus = process.waitFor();
        String result = IOUtils.toString(process.getInputStream());
        Thread.sleep(2000);
        Assert.assertEquals(inputData, result);
        Assert.assertEquals(125, exitStatus);
    }

    @Test
    public void testArgs() throws Exception {
        int statusCode = 5;
        SaveArgsCommand saveArgsCommand = new SaveArgsCommand(statusCode);
        String path = cmdUnit.declare(alias, saveArgsCommand);
        List<String> command = new ArrayList<>();
        command.add(path);
        command.addAll(args);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();
        new StreamRedirect(process.getErrorStream(), System.err).start();
        int exitStatus = process.waitFor();
        Assert.assertEquals(args, saveArgsCommand.getArgs());
        Assert.assertEquals(statusCode, exitStatus);
    }
}