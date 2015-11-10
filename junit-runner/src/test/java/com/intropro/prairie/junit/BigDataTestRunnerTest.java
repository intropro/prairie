package com.intropro.prairie.junit;

import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by presidentio on 11/10/15.
 */
@RunWith(BigDataTestRunner.class)
public class BigDataTestRunnerTest {

    @BigDataUnit
    private static SimpleUnit simpleStaticUnit;

    @BigDataUnit
    private SimpleUnit simpleUnit;

    private boolean simpleInitedBefore = false;
    private static boolean simpleInitedBeforeClass = false;

    @Before
    public void init(){
        simpleInitedBefore = simpleUnit != null && simpleUnit.isInited();
    }

    @BeforeClass
    public static void initClass(){
        simpleInitedBeforeClass = simpleStaticUnit != null && simpleStaticUnit.isInited();
    }

    @Test
    public void testInit() throws Exception {
        Assert.assertNotNull(simpleUnit);
        Assert.assertTrue(simpleUnit.isInited());
        Assert.assertTrue(simpleInitedBefore);
    }

    @Test
    public void testStatic() throws Exception {
        Assert.assertNotNull(simpleStaticUnit);
        Assert.assertTrue(simpleStaticUnit.isInited());
        Assert.assertTrue(simpleInitedBeforeClass);
    }
}