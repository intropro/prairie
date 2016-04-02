package com.intropro.prairie.unit.hbase;

import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.zookeeper.ZookeeperUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;

import java.io.IOException;

/**
 * Created by presidentio on 4/2/16.
 */
public class HBaseUnit extends HadoopUnit {

    @PrairieUnit
    private ZookeeperUnit zookeeperUnit;

    private HBaseTestingUtility hBaseTestingUtility;

    public HBaseUnit() {
        super("hbase");
    }

    @Override
    protected Configuration gatherConfigs() {
        Configuration conf = HBaseConfiguration.create(super.gatherConfigs());
        conf.addResource("hbase-site.prairie.xml");
        conf.set(HConstants.HBASE_DIR, getTmpDir().resolve("data").toUri().toString());
        conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperUnit.getHost() + ":" + zookeeperUnit.getPort());
        return conf;
    }

    @Override
    public Configuration getConfig() {
        return hBaseTestingUtility.getConfiguration();
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        try {
            hBaseTestingUtility.shutdownMiniHBaseCluster();
        } catch (IOException e) {
            throw new DestroyUnitException("Failed to shutdown hbase mini cluster", e);
        }
    }

    @Override
    protected void init() throws InitUnitException {
        hBaseTestingUtility = new HBaseTestingUtility(gatherConfigs());
        try {
            hBaseTestingUtility.startMiniHBaseCluster(1, 1);
        } catch (Exception e) {
            throw new InitUnitException("Failed to start hbase mini cluster", e);
        }
    }

}
