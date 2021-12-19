package com.intropro.prairie.unit.pig;

import com.intropro.prairie.format.avro.AvroFormat;
import com.intropro.prairie.format.json.JsonFormat;
import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by presidentio on 10/21/15.
 */
@RunWith(PrairieRunner.class)
public class PigUnitTest {

    @PrairieUnit
    private PigUnit pigUnit;

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @Test
    public void testPigUnit() throws Exception {
        hdfsUnit.saveAs(PigUnitTest.class.getClassLoader().getResourceAsStream("pig/input.csv"), "/data/input/part-00000",
                new TextFormat(), new TextFormat());
        String script = IOUtils.toString(PigUnitTest.class.getClassLoader().getResourceAsStream("pig/test.pig"));
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("INPUT_PATH", hdfsUnit.getNamenode() + "/data/input");
        placeholders.put("OUTPUT_PATH", hdfsUnit.getNamenode() + "/data/output");
        pigUnit.run(script, placeholders);
        hdfsUnit.compare(new Path("/data/output"), new TextFormat(), "pig/output.csv", new TextFormat()).assertEquals();
    }

    @Test
    public void testPigAvro() throws Exception {
        hdfsUnit.saveAs(PigUnitTest.class.getClassLoader().getResourceAsStream("pig-avro/input1.json"), "/data/input1/part-00000",
                new JsonFormat(), new AvroFormat("{\"type\":\"record\",\"name\":\"Test\"," +
                        "\"namespace\":\"com.intropro.prairie\"," +
                        "\"fields\":[" +
                        "{\"name\":\"field1\",\"type\":[\"string\",\"null\"]}," +
                        "{\"name\":\"field2\",\"type\":[\"string\",\"null\"]}," +
                        "{\"name\":\"field3\",\"type\":[\"string\",\"null\"]}," +
                        "{\"name\":\"FIELD\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}"));
        hdfsUnit.saveAs(PigUnitTest.class.getClassLoader().getResourceAsStream("pig-avro/input2.csv"), "/data/input2/part-00000",
                new TextFormat(), new TextFormat());
        String script = IOUtils.toString(PigUnitTest.class.getClassLoader().getResourceAsStream("pig-avro/test-avro.pig"));
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("INPUT_PATH1", "/data/input1");
        placeholders.put("INPUT_PATH2", "/data/input2");
        placeholders.put("OUTPUT_PATH", "/data/output");
        pigUnit.run(script, placeholders);
        hdfsUnit.compare(new Path("/data/output"), new TextFormat(), "pig-avro/output.csv", new TextFormat()).assertEquals();
    }
}