package com.intropro.prairie.format;

import com.intropro.prairie.format.avro.AvroFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by presidentio on 12/11/15.
 */
public class AvroFormatTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Format<Map<String, Object>> avroFormat;
    private Map<String, Object> record;

    @Before
    public void setUp() throws Exception {
        avroFormat = new AvroFormat(CommonKeyValueFormatTest.class.getClassLoader().getResourceAsStream("format/avro/avro.avsc"));
        record = new HashMap<>();
        record.put("field0", false);
        record.put("field1", 1);
        record.put("field2", "2");
        record.put("field3", 3L);
        record.put("field4", 4F);
        record.put("field5", 5D);
        record.put("field7", null);
        record.put("field8", 8);
        Map<String, String> record1 = new HashMap<>();
        record1.put("field9_field0", "9.0");
        record.put("field9", record1);
        record.put("field10", Arrays.asList(1,2,3,4,5));
        Map<String, List<Integer>> map2Map1 = new HashMap<>();
        map2Map1.put("11.0", Arrays.asList(11,12,13));
        record.put("field11", map2Map1);
    }

    @Test
    public void testRead() throws Exception {
        InputFormatReader<Map<String, Object>> reader = avroFormat.createReader(
                AvroFormatTest.class.getClassLoader().getResourceAsStream("format/avro/data.avro"));
        Map<String, Object> map1 = reader.next();
        Map<String, Object> map2 = reader.next();
        Map<String, Object> map3 = reader.next();
        reader.close();
        Assert.assertEquals(record, map1);
        Assert.assertEquals(record, map2);
        Assert.assertNull(map3);
    }

    @Test
    public void testReadWrite() throws Exception {
        File file = temporaryFolder.newFile();
        OutputStream outputStream = new FileOutputStream(file);
        OutputFormatWriter<Map<String, Object>> avroWriter = avroFormat.createWriter(outputStream);
        avroWriter.write(record);
        avroWriter.write(record);
        avroWriter.close();
        outputStream.close();
        InputStream inputStream = new FileInputStream(file);
        InputFormatReader<Map<String, Object>> reader = avroFormat.createReader(inputStream);
        Map<String, Object> map1 = reader.next();
        Map<String, Object> map2 = reader.next();
        Map<String, Object> map3 = reader.next();
        reader.close();
        inputStream.close();
        Assert.assertEquals(record, map1);
        Assert.assertEquals(record, map2);
        Assert.assertNull(map3);
    }
}
