package com.intropro.prairie.unit.cmd;

import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by presidentio on 11/14/15.
 */
@RunWith(BigDataTestRunner.class)
public class CmdUnitTest {

    @BigDataUnit
    private CmdUnit cmdUnit;

    private String alias = "cmd-unit";

    private String inputData = "123456asdfg\nzxcvvbn\n123445567";

    @Test
    public void testDeclare() throws Exception {
        String path = cmdUnit.declare(alias, new MirrorCommand());
        ProcessBuilder processBuilder = new ProcessBuilder(path);
        Process process = processBuilder.start();
        process.getOutputStream().write(inputData.getBytes());
        process.getOutputStream().close();
        String result = IOUtils.toString(process.getInputStream());
        Assert.assertEquals(inputData, result);
    }
}