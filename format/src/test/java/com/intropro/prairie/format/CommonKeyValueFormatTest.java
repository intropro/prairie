package com.intropro.prairie.format;

import com.intropro.prairie.format.sv.SvFormat;
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
public class CommonKeyValueFormatTest {

    @Parameterized.Parameters(name = "{index}: {0}({1})")
    public static Collection<Object[]> data() throws IOException {
        List<Format<Map<String, Object>>> formats = Arrays.<Format<Map<String, Object>>>asList(new SvFormat(','), new SvFormat('|'));
        List<Map<String, String>> data = new ArrayList<>();
        Map<String, String> raw = new HashMap<>();
        raw.put("field1", "value1");
        data.add(raw);
        raw = new HashMap<>();
        raw.put("field1", "value1");
        raw.put("field2", ",value1");
        data.add(raw);
        raw = new HashMap<>();
        raw.put("field1", "{\"key1\":\"val2\"}");
        raw.put("field2", "value2");
        data.add(raw);
        List<Object[]> parameters = new ArrayList<>(formats.size() * data.size());
        for (Format format : formats) {
            for (Map<String, String> stringStringMap : data) {
                parameters.add(new Object[]{format, stringStringMap});
            }
        }
        return parameters;
    }

    private Format<Map<String, String>> format;

    private Map<String, String> dataRaw;

    public CommonKeyValueFormatTest(Format<Map<String, String>> format, Map<String, String> dataRaw) {
        this.format = format;
        this.dataRaw = dataRaw;
    }

    @Test
    public void testCommon() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFormatWriter<Map<String, String>> outputFormatWriter = format.createWriter(outputStream);
        outputFormatWriter.write(dataRaw);
        outputFormatWriter.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        InputFormatReader<Map<String, String>> inputFormatReader = format.createReader(inputStream);
        Map<String, String> result = inputFormatReader.next();
        Assert.assertEquals("File content: " + outputStream.toString(), dataRaw, result);
    }
}