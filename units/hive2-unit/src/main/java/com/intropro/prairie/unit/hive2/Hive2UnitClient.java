package com.intropro.prairie.unit.hive2;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.CompareResponse;
import com.intropro.prairie.format.Format;
import com.intropro.prairie.format.InputFormatReader;
import com.intropro.prairie.format.exception.FormatException;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.utils.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 1/10/16.
 */
public class Hive2UnitClient implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(Hive2UnitClient.class);
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final long WAIT_START_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    public static final String USER = System.getProperty("user.name");

    private Connection connection;
    private Statement statement;
    private ByLineComparator byLineComparator = new ByLineComparator();
    private FileSystem fileSystem;
    private boolean open;

    public Hive2UnitClient(String host, int port, FileSystem fileSystem) throws IOException {
        this.fileSystem = fileSystem;
        initConnection(host, port);
    }

    public void initConnection(String host, int port) throws IOException {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new Error(e);
        }
        long startTime = System.currentTimeMillis();
        SQLException mostRecentException = null;
        while (connection == null && System.currentTimeMillis() - startTime < WAIT_START_TIMEOUT) {
            try {
                String jdbcUrl = String.format("jdbc:hive2://%s:%s/default", host, port);
                LOGGER.info("Connecting to " + jdbcUrl);
                connection = DriverManager.getConnection(jdbcUrl, USER, "");
                break;
            } catch (SQLException e) {
                mostRecentException = e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    throw new Error(e1);
                }
            }
        }
        if (connection == null) {
            throw new IOException("Failed to create hive connection", mostRecentException);
        }
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new IOException("Failed to create hive statement", e);
        }/*
        try {
            createDefaultDatabase();
        } catch (SQLException e) {
            LOGGER.warn("Failed to create default database: default", e);
        }*/
    }

    private void createDefaultDatabase() throws SQLException {
        statement.execute("CREATE DATABASE IF NOT EXISTS default");
    }

    public List<Map<String, String>> executeQuery(String script, Map<String, String> parameters) throws SQLException {
        script = StringUtils.replacePlaceholders(script, parameters);
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
        script = StringUtils.replacePlaceholders(script, parameters);
        execute(script);
    }


    public void execute(String script) throws SQLException {
        for (String scriptStatement : StatementsSplitter.splitStatements(script)) {
            LOGGER.info("Executing: " + scriptStatement);
            statement.execute(scriptStatement);
        }
    }

    public CompareResponse<Map<String, String>> compare(String query, InputStream expectedStream,
                                                        Format<Map<String, Object>> expectedFormat) throws IOException, SQLException {
        List<Map<String, String>> queryResult = executeQuery(query);
        try {
            InputFormatReader<Map<String, Object>> expectedReader = expectedFormat.createReader(expectedStream);
            CompareResponse<Map<String, String>> compareResponse = byLineComparator.compare(expectedReader.all(), queryResult);
            expectedReader.close();
            expectedReader.close();
            return compareResponse;
        } catch (FormatException e) {
            throw new IOException(e);
        }
    }

    public CompareResponse<Map<String, String>> compare(String query, Path expectedPath,
                                                        Format<Map<String, Object>> expectedFormat) throws IOException, SQLException {
        InputStream retrieved = fileSystem.open(expectedPath);
        return compare(query, retrieved, expectedFormat);
    }

    public CompareResponse<Map<String, String>> compare(String query, String expectedResource,
                                                        Format<Map<String, Object>> expectedFormat) throws IOException, SQLException {
        InputStream expectedStream = HdfsUnit.class.getClassLoader().getResourceAsStream(expectedResource);
        return compare(query, expectedStream, expectedFormat);
    }

    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        open = false;
        try {
            statement.close();
            connection.close();
        } catch (SQLException e) {
            throw new IOException("Failed to close hive connection", e);
        }
    }
}
