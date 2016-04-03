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
package com.intropro.prairie.unit.zookeeper;

import com.intropro.prairie.unit.common.BaseUnit;
import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by presidentio on 10/14/15.
 */
public class ZookeeperUnit extends BaseUnit {

    private static final Logger LOGGER = LogManager.getLogger(ZookeeperUnit.class);

    private ServerCnxnFactory cnxnFactory;
    private FileTxnSnapLog txnLog;
    private ZooKeeperServer zkServer;

    private Properties zkProperties;
    private int port;

    private Thread thread;

    public ZookeeperUnit() {
        super("zookeeper");
    }

    @Override
    protected void init() throws InitUnitException {
        port = PortProvider.nextPort();
        zkProperties = new Properties();
        zkProperties.put("clientPort", port);
        zkProperties.put("tickTime", 50);
        zkProperties.setProperty("dataDir", getTmpDir().resolve("data").toUri().toString());
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch (IOException | QuorumPeerConfig.ConfigException e) {
            throw new InitUnitException(e);
        }

        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);
        try {
            runFromConfig(configuration);
        } catch (IOException e) {
            LOGGER.error("Zookeeper failed", e);
        }
        thread = new Thread("zookeeper:" + port) {
            public void run() {
                try {
                    cnxnFactory.join();
                    if (zkServer.isRunning()) {
                        zkServer.shutdown();
                    }
                } catch (InterruptedException e) {
                    if (zkServer.isRunning()) {
                        zkServer.shutdown();
                    }
                }
            }
        };
        thread.start();
    }

    private void runFromConfig(ServerConfig config) throws IOException {
        zkServer = new ZooKeeperServer();
        try {

            txnLog = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir()));
            zkServer.setTxnLogFactory(txnLog);
            zkServer.setTickTime(config.getTickTime());
            zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
            zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            cnxnFactory = ServerCnxnFactory.createFactory();
            cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            cnxnFactory.startup(zkServer);
        } catch (InterruptedException e) {
            if (zkServer.isRunning()) {
                zkServer.shutdown();
            }
        }
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        thread.interrupt();
        if (zkServer.isRunning()) {
            zkServer.shutdown();
        }
        cnxnFactory.shutdown();
        try {
            txnLog.close();
        } catch (IOException e) {
            throw new DestroyUnitException(e);
        }
    }

    public String getHost() {
        return "localhost";
    }

    public int getPort() {
        return port;
    }
}
