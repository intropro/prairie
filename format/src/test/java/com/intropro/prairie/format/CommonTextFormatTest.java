package com.intropro.prairie.format;

import com.intropro.prairie.format.seq.SequenceFormat;
import com.intropro.prairie.format.text.TextFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by presidentio on 10/8/15.
 */
@RunWith(Parameterized.class)
public class CommonTextFormatTest {

    @Parameterized.Parameters(name = "{index}: {0}({1})")
    public static Collection<Object[]> data() throws IOException {
        List<Format<String>> formats = Arrays.asList(new TextFormat(), new SequenceFormat());
        List<String> data = new ArrayList<>();
        data.add("fasfasdas");
        data.add("scverhv ejfjwb wejfnjwne wfnwen wjenfwe");
        data.add("diwjejfwfjcfjsdv vniwefwiefjie wefnfewnwef wefnwefnwef wefk;189381723");
        List<Object[]> parameters = new ArrayList<>(formats.size() * data.size());
        for (Format format : formats) {
            for (String raw : data) {
                parameters.add(new Object[]{format, raw});
            }
        }
        return parameters;
    }

    private Format<String> format;

    private String dataRaw;

    public CommonTextFormatTest(Format<String> format, String dataRaw) {
        this.format = format;
        this.dataRaw = dataRaw;
    }

    @Test
    public void testCommon() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFormatWriter<String> outputFormatWriter = format.createWriter(outputStream);
        outputFormatWriter.write(dataRaw);
        outputFormatWriter.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        InputFormatReader<String> inputFormatReader = format.createReader(inputStream);
        String result = inputFormatReader.next();
        Assert.assertEquals("File content: " + outputStream.toString(), dataRaw, result);
    }
}