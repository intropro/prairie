package com.intropro.prairie.unit.hbase;

import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.zookeeper.ZookeeperUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by presidentio on 4/2/16.
 */
public class HBaseUnit extends HadoopUnit {

    private static final Logger LOGGER = LogManager.getLogger(HBaseUnit.class);

    @PrairieUnit
    private ZookeeperUnit zookeeperUnit;

    private HBaseTestingUtility hBaseTestingUtility;

    public HBaseUnit() {
        super("hbase");
    }

    @Override
    protected Configuration gatherConfigs() {
        System.setProperty("test.build.data.basedirectory", getTmpDir().resolve("data").toString());
        Configuration conf = HBaseConfiguration.create(super.gatherConfigs());
        conf.addResource("hbase-site.prairie.xml");
        conf.setInt(HConstants.MASTER_PORT, PortProvider.nextPort());
        conf.setInt(HConstants.MASTER_INFO_PORT, PortProvider.nextPort());
        conf.setInt(HConstants.REGIONSERVER_PORT, PortProvider.nextPort());
        conf.setInt(HConstants.REGIONSERVER_INFO_PORT, PortProvider.nextPort());
        conf.set(HConstants.HBASE_DIR, getTmpDir().resolve("root").toString());
        conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperUnit.getHost() + ":" + zookeeperUnit.getPort());
        conf.set(HConstants.ZK_CFG_PROPERTY_PREFIX + HConstants.CLIENT_PORT_STR, "" + zookeeperUnit.getPort());
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
            FileSystem.get(hBaseTestingUtility.getConfiguration()).delete(hBaseTestingUtility.getDefaultRootDirPath(),
                    true);
        } catch (IOException e) {
            throw new DestroyUnitException("Failed to shutdown hbase mini cluster", e);
        }
    }

    @Override
    protected void init() throws InitUnitException {
        hBaseTestingUtility = new HBaseTestingUtility(gatherConfigs());
        try {
            FileSystem.get(hBaseTestingUtility.getConfiguration()).delete(hBaseTestingUtility.getDefaultRootDirPath(),
                    true);
        } catch (IOException e) {
            LOGGER.error(e);
        }
        try {
            hBaseTestingUtility.startMiniHBaseCluster(1, 1);
        } catch (Exception e) {
            throw new InitUnitException("Failed to start hbase mini cluster", e);
        }
    }

    public static void main(String[] args) {
        String s = "acada";
        System.out.println(s.substring(4, 5));
    }

}
