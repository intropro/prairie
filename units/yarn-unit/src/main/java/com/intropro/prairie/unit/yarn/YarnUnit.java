/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intropro.prairie.unit.yarn;

import com.intropro.prairie.unit.cmd.CmdUnit;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by presidentio on 04.09.15.
 */
public class YarnUnit extends HadoopUnit {

    private static final Logger LOGGER = LogManager.getLogger(YarnUnit.class);

    private static final String NAME = "prairie-yarn";

    private MiniMRYarnCluster miniMR;

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @PrairieUnit
    private CmdUnit cmdUnit;

    public YarnUnit() {
        super("yarn");
    }

    @Override
    public void init() throws InitUnitException {
        miniMR = new MiniMRYarnCluster(NAME);
        miniMR.init(gatherConfigs());
        miniMR.start();
    }

    @Override
    protected YarnConfiguration gatherConfigs() {
        YarnConfiguration yarnConfigs = new YarnConfiguration(super.gatherConfigs());
        yarnConfigs.set("fs.defaultFS", hdfsUnit.getNamenode());
        yarnConfigs.set("mapreduce.task.tmp.dir", getTmpDir().toString());
        String user = System.getProperty("user.name");
        yarnConfigs.set("hadoop.proxyuser." + user + ".hosts", "*");
        yarnConfigs.set("hadoop.proxyuser." + user + ".groups", "*");
        yarnConfigs.set("yarn.nodemanager.admin-env", "PATH=$PATH:" + cmdUnit.getPath());
        yarnConfigs.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        yarnConfigs.addResource("mapred-site.prairie.xml");
        yarnConfigs.addResource("yarn-site.prairie.xml");
        return yarnConfigs;
    }

    @Override
    public void destroy() throws DestroyUnitException {
        if (miniMR != null) {
            miniMR.stop();
        }
    }

    @Override
    public Configuration getConfig() {
        Configuration configuration = new Configuration(miniMR.getConfig());
        configuration.set("mapreduce.framework.name", gatherConfigs().get("mapreduce.framework.name"));
        return configuration;
    }

    public String getJobTracker() {
        return getConfig().get(YarnConfiguration.RM_ADDRESS);
    }
}
