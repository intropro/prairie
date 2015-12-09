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
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by presidentio on 04.09.15.
 */
public class YarnUnit extends HadoopUnit {

    private static final String NAME = "prairie-yarn";

    private MiniYARNCluster miniMR;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    @BigDataUnit
    private CmdUnit cmdUnit;

    public YarnUnit() {
        super("yarn");
    }

    @Override
    public void init() throws InitUnitException {
        YarnConfiguration bootConf = new YarnConfiguration(createConfig());
        try {
            bootConf.addResource(hdfsUnit.getFileSystem().getConf());
        } catch (IOException e) {
            throw new InitUnitException("Failed to get file system", e);
        }
        bootConf.set("mapreduce.task.tmp.dir", getTmpDir().toString());
        String user = System.getProperty("user.name");
        bootConf.set("hadoop.proxyuser." + user + ".hosts", "*");
        bootConf.set("hadoop.proxyuser." + user + ".groups", "*");
        bootConf.set("yarn.nodemanager.admin-env", "PATH=$PATH:" + cmdUnit.getPath());
        bootConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        bootConf.addResource("yarn-site.xml");
        bootConf.addResource("mapred-site.xml");
        miniMR = new MiniMRYarnCluster(NAME);
        miniMR.init(bootConf);
        miniMR.start();
    }

    @Override
    public void destroy() throws DestroyUnitException {
        miniMR.stop();
    }

    public Configuration getConfig() {
        return new Configuration(miniMR.getConfig());
    }

    public void dumpConfigs(File confDir) throws IOException {
        FileWriter configWriter = new FileWriter(new File(confDir, "yarn-site.xml"));
        getConfig().writeXml(configWriter);
        configWriter.close();
    }
}
