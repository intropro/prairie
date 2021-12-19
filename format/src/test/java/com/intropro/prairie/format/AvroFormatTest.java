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
    private Map<String, Object> record1;
    private Map<String, Object> record2;

    @Before
    public void setUp() throws Exception {
        avroFormat = new AvroFormat(CommonKeyValueFormatTest.class.getClassLoader().getResourceAsStream("format/avro/avro.avsc"));
        record1 = new HashMap<>();
        record1.put("field0", false);
        record1.put("field1", 1);
        record1.put("field2", "2");
        record1.put("field3", 3L);
        record1.put("field4", 4F);
        record1.put("field5", 5D);
        record1.put("field7", null);
        record1.put("field8", 8);
        Map<String, String> subRecord1 = new HashMap<>();
        subRecord1.put("field9_field0", "9.0");
        record1.put("field9", subRecord1);
        record1.put("field10", Arrays.asList(1,2,3,4,5));
        Map<String, List<Integer>> map2Map1 = new HashMap<>();
        map2Map1.put("11.0", Arrays.asList(11,12,13));
        record1.put("field11", map2Map1);
        record1.put("field12", "12");
        record2 = new HashMap<>(this.record1);
        record2.put("field12", 12);
    }

    @Test
    public void testRead() throws Exception {
        InputFormatReader<Map<String, Object>> reader = avroFormat.createReader(
                AvroFormatTest.class.getClassLoader().getResourceAsStream("format/avro/data.avro"));
        Map<String, Object> map1 = reader.next();
        Map<String, Object> map2 = reader.next();
        Map<String, Object> map3 = reader.next();
        reader.close();
        Assert.assertEquals(record1, map1);
        Assert.assertEquals(record2, map2);
        Assert.assertNull(map3);
    }

    @Test
    public void testReadWrite() throws Exception {
        File file = temporaryFolder.newFile();
        OutputStream outputStream = new FileOutputStream(file);
        OutputFormatWriter<Map<String, Object>> avroWriter = avroFormat.createWriter(outputStream);
        avroWriter.write(record1);
        avroWriter.write(record2);
        avroWriter.close();
        outputStream.close();
        InputStream inputStream = new FileInputStream(file);
        InputFormatReader<Map<String, Object>> reader = avroFormat.createReader(inputStream);
        Map<String, Object> map1 = reader.next();
        Map<String, Object> map2 = reader.next();
        Map<String, Object> map3 = reader.next();
        reader.close();
        inputStream.close();
        Assert.assertEquals(record1, map1);
        Assert.assertEquals(record2, map2);
        Assert.assertNull(map3);
    }
}
