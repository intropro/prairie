package com.intropro.prairie.unit.pig;

import com.intropro.prairie.format.text.TextFormat;
import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
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
@RunWith(BigDataTestRunner.class)
public class PigUnitTest {

    @BigDataUnit
    private PigUnit pigUnit;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    @Test
    public void testPigUnit() throws Exception {
        hdfsUnit.saveAs(PigUnitTest.class.getClassLoader().getResourceAsStream("input.csv"), "/data/input/part-00000",
                new TextFormat(), new TextFormat());
        String script = IOUtils.toString(PigUnitTest.class.getClassLoader().getResourceAsStream("test.pig"));
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("INPUT_PATH", "/data/input");
        placeholders.put("OUTPUT_PATH", "/data/output");
        pigUnit.run(script, placeholders);
        hdfsUnit.compare(new Path("/data/output"), new TextFormat(), "output.csv", new TextFormat());
    }
}