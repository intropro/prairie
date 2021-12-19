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
package com.intropro.prairie.unit.hive2;

import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.Version;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.yarn.YarnUnit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hive.service.server.HiveServer2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Created by presidentio on 07.09.15.
 */
public class Hive2Unit extends HadoopUnit {

    private static final Logger LOGGER = LogManager.getLogger(Hive2Unit.class);

    public static final Version VERSION = getVersion("org.apache.hive", "hive-common");

    public static final String HIVE_HOME = "/user/hive";
    public static final String HIVE_HOST = "localhost";

    private HiveServer2 hiveServer;
    private List<Hive2UnitClient> clients = new ArrayList<>();

    @PrairieUnit
    private YarnUnit yarnUnit;

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    private int port;
    private int metastorePort;
    private String jdbcUrl;
    private String metastoreJdbcUrl;

    public Hive2Unit() {
        super("hive");
    }

    @Override
    public void init() throws InitUnitException {
        try {
            hdfsUnit.getFileSystem().mkdirs(new Path(HIVE_HOME));
            hdfsUnit.getFileSystem().setOwner(new Path(HIVE_HOME), "hive", "hive");
        } catch (IOException e) {
            throw new InitUnitException("Failed to create hive home directory: " + HIVE_HOME, e);
        }
        metastorePort = PortProvider.nextPort();
        final HiveConf hiveConf = gatherConfigs();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //TODO: remove static call
                    HiveMetaStore.startMetaStore(metastorePort, null, hiveConf);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }).start();
        hiveServer = new HiveServer2();
        hiveServer.init(hiveConf);
        hiveServer.start();
        jdbcUrl = String.format("jdbc:hive2://%s:%s/default", HIVE_HOST, port);
    }

    @Override
    protected HiveConf gatherConfigs() {
        port = PortProvider.nextPort();
        HiveConf hiveConf = new HiveConf(super.gatherConfigs(), Hive2Unit.class);
        Iterator<Map.Entry<String, String>> iterator = yarnUnit.getConfig().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            hiveConf.set(next.getKey(), next.getValue());
        }
        hiveConf.addResource("hive-site.prairie.xml");
        hiveConf.setVar(HiveConf.ConfVars.SCRATCHDIR, getTmpDir().resolve("scratchdir").toString());
        hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, getTmpDir().resolve("warehouse").toString());
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, HIVE_HOST);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
                "thrift://" + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST) + ":" + metastorePort);
        metastoreJdbcUrl = "jdbc:hsqldb:mem:" + UUID.randomUUID().toString();
        hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, metastoreJdbcUrl + ";create=true");
        return hiveConf;
    }

    @Override
    public void destroy() throws DestroyUnitException {
        for (Hive2UnitClient client : clients) {
            if (client.isOpen()) {
                try {
                    client.close();
                } catch (IOException e) {
                    throw new DestroyUnitException("Failed to close hive client", e);
                }
            }
        }
        hiveServer.stop();
    }

    public Hive2UnitClient createClient() throws IOException {
        Hive2UnitClient hive2UnitClient = new Hive2UnitClient(HIVE_HOST, port, hdfsUnit.getFileSystem());
        clients.add(hive2UnitClient);
        return hive2UnitClient;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public HiveConf getConfig() {
        HiveConf hiveConf = new HiveConf(hiveServer.getHiveConf());
        return hiveConf;
    }

}
