package com.intropro.prairie.unit.zookeeper;

import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by presidentio on 10/14/15.
 */
@RunWith(PrairieRunner.class)
public class ZookeeperUnitTest {

    @PrairieUnit
    private ZookeeperUnit zookeeperUnit;

    @Test
    public void testZookeeperUnit() throws Exception {
        Assert.assertNotNull(zookeeperUnit.getHost());
        Assert.assertNotNull(zookeeperUnit.getPort());
    }
}