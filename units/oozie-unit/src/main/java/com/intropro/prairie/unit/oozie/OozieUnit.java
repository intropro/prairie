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
package com.intropro.prairie.unit.oozie;

import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.yarn.YarnUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.DagEngine;
import org.apache.oozie.LocalOozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.service.*;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.util.XConfiguration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by presidentio on 9/13/15.
 */
public class OozieUnit extends HadoopUnit {

    private static final List<String> EXCLUDED_SERVICES = Arrays.asList("org.apache.oozie.service.RecoveryService",
            "org.apache.oozie.service.PurgeService", "org.apache.oozie.service.ShareLibService");

    private EmbeddedServletContainer container;

    private Services services;

    private File actionConfDir;
    private File hadoopConfDir;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    @BigDataUnit
    private YarnUnit yarnUnit;

    private org.apache.oozie.client.OozieClient oozieClient;

    private String user = System.getProperty("user.name");

    public OozieUnit() {
        super("oozie");
    }

    @Override
    public void init() throws InitUnitException {
        File confDir = new File(getTmpDir().toFile(), "conf");
        confDir.mkdirs();
        File dataDir = new File(getTmpDir().toFile(), "data");
        dataDir.mkdirs();

        System.setProperty("oozie.test.metastore.server", "false");
        System.setProperty("oozie.data.dir", dataDir.getAbsolutePath());
        System.setProperty("oozie.home.dir", getTmpDir().toString());
        try {
            writeActionConfigs();
            writeHadoopConfigs();
        } catch (IOException e) {
            throw new InitUnitException("Failed to dump yarn configs", e);
        }

        try {
            services = new Services();
            services.getConf().set("oozie.service.HadoopAccessorService.hadoop.configurations",
                    "*=" + hadoopConfDir.getAbsolutePath());
            services.getConf().set("oozie.service.HadoopAccessorService.action.configurations",
                    "*=" + actionConfDir.getAbsolutePath());
            XConfiguration.copy(gatherConfigs(), services.getConf());
            String classes = services.getConf().get(Services.CONF_SERVICE_CLASSES);
            for (String excludedService : EXCLUDED_SERVICES) {
                classes = classes.replaceAll(excludedService + "\\s?,?", "");
            }
            services.getConf().set(Services.CONF_SERVICE_CLASSES, classes);
            services.init();
        } catch (ServiceException e) {
            throw new InitUnitException("Failed to start oozie component", e);
        }

        container = new EmbeddedServletContainer("oozie");
        container.addServletEndpoint("/callback", CallbackServlet.class);
        try {
            container.start();
        } catch (Exception e) {
            throw new InitUnitException("Failed to start oozie servlet", e);
        }
        String callbackUrl = container.getServletURL("/callback");
        services.getConf().set(CallbackService.CONF_BASE_URL, callbackUrl);
        oozieClient = getClient();
    }

    @Override
    protected Configuration gatherConfigs() {
        Configuration configuration = super.gatherConfigs();
        configuration.addResource("oozie-site.prairie.xml");
        return configuration;
    }

    private void writeActionConfigs() throws IOException {
        actionConfDir = new File(getTmpDir().toFile(), "action-conf");
        File defaultActionConfDir = new File(actionConfDir, "default");
        defaultActionConfDir.mkdirs();
        FileOutputStream fileOutputStream = new FileOutputStream(new File(defaultActionConfDir, "yarn-site.xml"));
        yarnUnit.getConfig().writeXml(fileOutputStream);
        fileOutputStream.close();
        fileOutputStream = new FileOutputStream(new File(defaultActionConfDir, "hdfs-site.xml"));
        hdfsUnit.getConfig().writeXml(fileOutputStream);
        fileOutputStream.close();
    }

    private void writeHadoopConfigs() throws IOException {
        hadoopConfDir = new File(getTmpDir().toFile(), "hadoop-conf");
        new File(hadoopConfDir, "default").mkdirs();
        Configuration yarnConfigs = yarnUnit.getConfig();
        yarnConfigs.set("mapreduce.framework.name", "yarn");
        FileOutputStream fileOutputStream = new FileOutputStream(new File(hadoopConfDir, "yarn-site.xml"));
        yarnConfigs.writeXml(fileOutputStream);
        fileOutputStream.close();
        fileOutputStream = new FileOutputStream(new File(hadoopConfDir, "hdfs-site.xml"));
        hdfsUnit.getConfig().writeXml(fileOutputStream);
        fileOutputStream.close();
    }

    @Override
    public Configuration getConfig() {
        return services.getConf();
    }

    @Override
    public void destroy() throws DestroyUnitException {
        container.stop();
        services.destroy();
    }

    public org.apache.oozie.client.OozieClient getClient() {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user);
        return new LocalOozieClient(dagEngine);
    }

    public OozieJob run(Properties properties) throws OozieClientException {
        properties.setProperty(org.apache.oozie.client.OozieClient.USER_NAME, user);
        return new OozieJob(oozieClient.run(properties), oozieClient);
    }

}
