package com.intropro.prairie.unit.zookeeper;

import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by presidentio on 10/14/15.
 */
@RunWith(BigDataTestRunner.class)
public class ZookeeperUnitTest {

    @BigDataUnit
    private ZookeeperUnit zookeeperUnit;

    @Test
    public void testZookeeperUnit() throws Exception {
        Assert.assertNotNull(zookeeperUnit.getHost());
        Assert.assertNotNull(zookeeperUnit.getPort());
    }
}