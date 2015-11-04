package com.intropro.prairie.comparator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by presidentio on 10/13/15.
 */
@RunWith(Parameterized.class)
public class ByLineComparatorTest {

    private ByLineComparator<String> byLineComparator = new ByLineComparator<>();

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{Arrays.asList("a", "b"), Arrays.asList("a", "b"), true},
                new Object[]{Arrays.asList("a", "b"), Arrays.asList("b", "a"), true},
                new Object[]{Arrays.asList("a", "b"), Arrays.asList("a", "b", "c"), false},
                new Object[]{Arrays.asList("a", "b"), Arrays.asList("a", "b", "a"), false},
                new Object[]{Arrays.asList("a", "b", "c"), Arrays.asList("a", "b"), false},
                new Object[]{Arrays.asList(), Arrays.asList(), true});
    }

    private List<String> expected;
    private List<String> retrieved;
    private boolean equals;

    public ByLineComparatorTest(List<String> expected, List<String> retrieved, boolean equals) {
        this.expected = expected;
        this.retrieved = retrieved;
        this.equals = equals;
    }

    @Test
    public void testCompare() throws Exception {
        CompareResponse<String> compareResponse = byLineComparator.compare(expected, retrieved);
        if (equals) {
            compareResponse.assertEquals();
        } else {
            compareResponse.assertNotEquals();
        }
    }
}