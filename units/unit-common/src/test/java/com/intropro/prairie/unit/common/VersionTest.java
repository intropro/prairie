package com.intropro.prairie.unit.common;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by presidentio on 3/2/16.
 */
@RunWith(Parameterized.class)
public class VersionTest {

    @Parameterized.Parameters(name = "{index}: {0} vs {1} should be {2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{"1.0.0", "1.0.1", -1},
                new Object[]{"1.0.0", "1.1.0", -1},
                new Object[]{"1.0.0", "2.0.0", -1},
                new Object[]{"1.0.0", "1.0.0.1", -1},
                new Object[]{"1.0.1", "1.0.0", 1},
                new Object[]{"1.1.0", "1.0.0", 1},
                new Object[]{"2.0.0", "1.0.0", 1},
                new Object[]{"1.0.0.1", "1.0.0", 1},
                new Object[]{"1.0.0-cdh5", "1.0.0-cdh5", 0},
                new Object[]{"1.0.0-cdh5", "1.0.0-cdh6", -1},
                new Object[]{"1.0.0-cdh5.1", "1.0.0-cdh5.2", -1},
                new Object[]{"1.0.0-cdh6", "1.0.0-cdh5", 1},
                new Object[]{"1.0.0-cdh5.2", "1.0.0-cdh5.1", 1},
                new Object[]{"1.0.0-cdh5.1.1", "1.0.0-cdh5.1", 1},
                new Object[]{"1.0.0", "1.0.0", 0}
        );
    }

    private String version1;
    private String version2;
    private int expectedResult;

    public VersionTest(String version1, String version2, int expectedResult) {
        this.version1 = version1;
        this.version2 = version2;
        this.expectedResult = expectedResult;
    }

    @Test
    public void testCompareTo() throws Exception {
        int result = new Version(version1).compareTo(new Version(version2));
        if (result == 0) {
            Assert.assertEquals(expectedResult, result);
        } else {
            Assert.assertTrue(expectedResult * result > 0);
        }
    }
}