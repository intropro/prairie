/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.oozie.DagEngine;
import org.apache.oozie.LocalOozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.service.*;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.test.EmbeddedServletContainer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by presidentio on 9/13/15.
 */
public class OozieUnit extends HadoopUnit {

    private EmbeddedServletContainer container;

    private Services services;

    private File actionConfDir;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    @BigDataUnit
    private YarnUnit yarnUnit;

    private org.apache.oozie.client.OozieClient oozieClient;

    public OozieUnit() {
        super("oozie");
    }

    @Override
    public void init() throws InitUnitException {
        File confDir = new File(getTmpDir().toFile(), "conf");
        confDir.mkdirs();
        File dataDir = new File(getTmpDir().toFile(), "data");
        dataDir.mkdirs();
        actionConfDir = new File(getTmpDir().toFile(), "action-conf");
        actionConfDir.mkdirs();

        System.setProperty("derby.stream.error.file", new File(getTmpDir().toFile(), "derby.log").getAbsolutePath());

        try {
            hdfsUnit.getFileSystem().mkdirs(new org.apache.hadoop.fs.Path("/user/oozie"));
            hdfsUnit.getFileSystem().setOwner(new org.apache.hadoop.fs.Path("/user/oozie"), "oozie", "oozie");
        } catch (IOException e) {
            throw new InitUnitException("Failed on hdfs directories initialization", e);
        }
        System.setProperty("oozie.test.metastore.server", "false");
        System.setProperty("oozie.data.dir", dataDir.getAbsolutePath());
        System.setProperty("oozie.home.dir", getTmpDir().toString());
        System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        String log4jFile = System.getProperty(XLogService.LOG4J_FILE, null);
        if (log4jFile == null) {
            System.setProperty(XLogService.LOG4J_FILE, "localoozie-log4j.properties");
        }
        File yarnConfigFile;
        try {
            yarnConfigFile = yarnUnit.dumpConfigs();
        } catch (IOException e) {
            throw new InitUnitException("Failed to dump yarn configs", e);
        }

        try {
            services = new Services();
            services.getConf().set("oozie.service.HadoopAccessorService.hadoop.configurations",
                    "*=" + yarnConfigFile.getParent());
            services.getConf().set("oozie.service.HadoopAccessorService.action.configurations",
                    "*=" + actionConfDir.getAbsolutePath());
            services.getConf().set(JPAService.CONF_CREATE_DB_SCHEMA, "true");
            services.getConf().set("mapred.job.tracker", YarnConfiguration.RM_ADDRESS);
            services.getConf().set("mapreduce.framework.name", "yarn");
            services.getConf().addResource(yarnUnit.getConfig());
            services.getConf().addResource(hdfsUnit.getFileSystem().getConf());
            String classes = services.getConf().get(Services.CONF_SERVICE_CLASSES);
            services.getConf().set(Services.CONF_SERVICE_CLASSES, classes.replaceAll("org.apache.oozie.service.ShareLibService,", ""));
            services.init();
        } catch (ServiceException e) {
            throw new InitUnitException("Failed to start oozie component", e);
        } catch (IOException e) {
            throw new InitUnitException("Failed to get file system", e);
        }

        if (log4jFile != null) {
            System.setProperty(XLogService.LOG4J_FILE, log4jFile);
        } else {
            System.getProperties().remove(XLogService.LOG4J_FILE);
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
        UserGroupInformation.createUserForTesting("hive", new String[]{"hive"});
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
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine("oozie");
        return new LocalOozieClient(dagEngine);
    }

    public OozieJob run(Properties properties) throws OozieClientException {
        properties.setProperty(org.apache.oozie.client.OozieClient.USER_NAME, "oozie");
        return new OozieJob(oozieClient.run(properties), oozieClient);
    }

}
