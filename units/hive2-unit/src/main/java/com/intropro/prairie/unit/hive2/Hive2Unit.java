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

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.CompareResponse;
import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.Version;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.format.Format;
import com.intropro.prairie.format.InputFormatReader;
import com.intropro.prairie.unit.yarn.YarnUnit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 07.09.15.
 */
public class Hive2Unit extends HadoopUnit {

    private static final Logger LOGGER = LogManager.getLogger(Hive2Unit.class);

    public static final Version VERSION = getVersion();

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final long WAIT_START_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

    public static final String HIVE_MODE = "binary";
    public static final String HIVE_USER = "hive";
    public static final String HIVE_GROUP = "hive";
    public static final String HIVE_HOST = "localhost";

    public static final String HIVE_HOME = "/user/hive";

    private HiveServer2 hiveServer;

    @BigDataUnit
    private YarnUnit yarnUnit;

    @BigDataUnit
    private HdfsUnit hdfsUnit;

    private ByLineComparator byLineComparator = new ByLineComparator();

    private Connection connection;
    private Statement statement;
    private String jdbcUrl;
    private int port;
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
        port = PortProvider.nextPort();
        HiveConf hiveConf = new HiveConf(yarnUnit.getConfig(), Hive2Unit.class);
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, HIVE_MODE);
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, HIVE_HOST);
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
        hiveConf.set("datanucleus.connectiondrivername", "org.hsqldb.jdbc.JDBCDriver");
        hiveConf.set("datanucleus.connectionPoolingType", "None");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.hsqldb.jdbc.JDBCDriver");
        hiveConf.setVar(HiveConf.ConfVars.HADOOPBIN, "NO_BIN!");
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVEMETADATAONLYQUERIES, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVESKEWJOIN, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.LOCALMODEAUTO, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.LOCALMODEAUTO, false);
        hiveConf.set("hive.exec.submit.local.task.via.child", "false");
        metastoreJdbcUrl = "jdbc:hsqldb:mem:" + UUID.randomUUID().toString();
        hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, metastoreJdbcUrl + ";create=true");
        hiveServer = new HiveServer2();
        hiveServer.init(hiveConf);
        hiveServer.start();
        initConnection();
    }

    public void initConnection() throws InitUnitException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new InitUnitException("Could not load hive jdbc driver", e);
        }
        long startTime = System.currentTimeMillis();
        SQLException mostRecentException = null;
        while (connection == null && System.currentTimeMillis() - startTime < WAIT_START_TIMEOUT) {
            try {
                int hivePort = getConfig().getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
                jdbcUrl = String.format("jdbc:hive2://%s:%s/default", HIVE_HOST, hivePort);
                LOGGER.info("Connecting to " + jdbcUrl);
                connection = DriverManager.getConnection(jdbcUrl, HIVE_USER, "");
                break;
            } catch (SQLException e) {
                mostRecentException = e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    throw new InitUnitException(e1);
                }
            }
        }
        if (connection == null) {
            throw new InitUnitException("Failed to create hive connection", mostRecentException);
        }
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new InitUnitException("Failed to create hive statement", e);
        }
        try {
            createDefaultDatabase();
        } catch (SQLException e) {
            LOGGER.warn("Failed to create default database: default", e);
        }
    }

    private void createDefaultDatabase() throws SQLException {
        statement.execute("CREATE DATABASE IF NOT EXISTS default");
    }

    @Override
    public void destroy() throws DestroyUnitException {
        try {
            statement.close();
            connection.close();
        } catch (SQLException e) {
            throw new DestroyUnitException("Failed to close hive connection", e);
        }
        hiveServer.stop();
        try {
            DriverManager.getConnection(
                    metastoreJdbcUrl + ";shutdown=true", "SA", "");
        } catch (SQLException e) {
            LOGGER.warn("Failed to clean metastore db", e);
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public HiveConf getConfig() {
        HiveConf hiveConf = new HiveConf(hiveServer.getHiveConf());
        hiveConf.addResource("hive-site.xml");
        return hiveConf;
    }

    public List<Map<String, String>> executeQuery(String script, Map<String, String> parameters) throws SQLException {
        script = replacePlaceholders(script, parameters);
        return executeQuery(script);
    }

    public List<Map<String, String>> executeQuery(String script) throws SQLException {
        List<Map<String, String>> result = new ArrayList<Map<String, String>>();
        List<String> splittedScript = StatementsSplitter.splitStatements(script);
        for (String scriptStatement : splittedScript.subList(0, splittedScript.size() - 1)) {
            LOGGER.info("Executing: " + scriptStatement);
            statement.execute(scriptStatement);
        }
        String query = splittedScript.get(splittedScript.size() - 1);
        LOGGER.info("Executing: " + query);
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            Map<String, String> row = new LinkedHashMap<String, String>();
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                row.put(resultSet.getMetaData().getColumnName(i), resultSet.getString(i));
            }
            result.add(row);
        }
        return result;
    }

    public void execute(String script, Map<String, String> parameters) throws SQLException {
        script = replacePlaceholders(script, parameters);
        execute(script);
    }


    public void execute(String script) throws SQLException {
        for (String scriptStatement : StatementsSplitter.splitStatements(script)) {
            LOGGER.info("Executing: " + scriptStatement);
            statement.execute(scriptStatement);
        }
    }

    public Path createDataDir(String path) throws IOException {
        Path dataDirPath = new Path(path);
        hdfsUnit.getFileSystem().mkdirs(dataDirPath);
        hdfsUnit.getFileSystem().setOwner(dataDirPath, HIVE_USER, HIVE_GROUP);
        return dataDirPath;
    }

    public CompareResponse<Map<String, String>> compare(String query, InputStream expectedStream,
                                                        Format<Map<String, String>> expectedFormat) throws IOException, SQLException {
        List<Map<String, String>> queryResult = executeQuery(query);
        InputFormatReader<Map<String, String>> expectedReader = expectedFormat.createReader(expectedStream);
        CompareResponse<Map<String, String>> compareResponse = byLineComparator.compare(expectedReader.all(), queryResult);
        expectedReader.close();
        expectedReader.close();
        return compareResponse;
    }

    public CompareResponse<Map<String, String>> compare(String query, Path expectedPath,
                                                        Format<Map<String, String>> expectedFormat) throws IOException, SQLException {
        InputStream retrieved = hdfsUnit.getFileSystem().open(expectedPath);
        return compare(query, retrieved, expectedFormat);
    }

    public CompareResponse<Map<String, String>> compare(String query, String expectedResource,
                                                        Format<Map<String, String>> expectedFormat) throws IOException, SQLException {
        InputStream expectedStream = HdfsUnit.class.getClassLoader().getResourceAsStream(expectedResource);
        return compare(query, expectedStream, expectedFormat);
    }

    private static Version getVersion() {
        Properties properties = new Properties();
        try {
            properties.load(Hive2Unit.class.getClassLoader().getResourceAsStream("META-INF/maven/org.apache.hive/hive-common/pom.properties"));
        } catch (IOException e) {
            LOGGER.error("Can't load hive version", e);
        }
        return new Version(properties.getProperty("version", "unknown"));
    }

}
