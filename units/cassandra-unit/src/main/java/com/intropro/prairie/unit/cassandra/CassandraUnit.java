package com.intropro.prairie.unit.cassandra;

import com.intropro.prairie.unit.common.BaseUnit;
import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.*;
import org.yaml.snakeyaml.representer.Representer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by presidentio on 10/4/16.
 */
public class CassandraUnit extends BaseUnit implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(CassandraUnit.class);

    private CassandraDaemon cassandraDaemon;
    private Thread thread;

    private String host = "localhost";

    public CassandraUnit() {
        super("cassandra");
    }

    @Override
    protected void init() throws InitUnitException {
        prepareConfig();
        cassandraDaemon = new CassandraDaemon();
        try {
            cassandraDaemon.init(null);
        } catch (IOException e) {
            throw new InitUnitException("Failed to init cassandra demon", e);
        }
        thread = new Thread(this);
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void run() {
        cassandraDaemon.start();
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        thread.interrupt();
        cassandraDaemon.stop();
    }

    public CassandraClient getClient() {
        return new CassandraClient(DatabaseDescriptor.getListenAddress().getCanonicalHostName(),
                DatabaseDescriptor.getNativeTransportPort());
    }

    private Config prepareConfig() throws InitUnitException {
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName().toLowerCase();
        } catch (UnknownHostException e) {
            LOGGER.warn("Failed to get canonical host name. Using default: " + host);
        }
        Representer representer = new Representer() {
            @Override
            protected NodeTuple representJavaBeanProperty(Object javaBean, Property property, Object propertyValue, Tag customTag) {
                if (propertyValue == null) {
                    return null;
                } else {
                    if (propertyValue instanceof ParameterizedClass) {
                        ParameterizedClass parameterizedClass = (ParameterizedClass) propertyValue;
                        NodeTuple classNameField = new NodeTuple(representData("class_name"),
                                representData(parameterizedClass.class_name));
                        List<NodeTuple> parametersNodes = new ArrayList<>();
                        for (Map.Entry<String, String> stringStringEntry : parameterizedClass.parameters.entrySet()) {
                            parametersNodes.add(new NodeTuple(representData(stringStringEntry.getKey()),
                                    representData(stringStringEntry.getValue())));
                        }
                        NodeTuple parametersField = new NodeTuple(representData("parameters"),
                                new SequenceNode(Tag.SEQ, Collections.<Node>singletonList(
                                        new MappingNode(Tag.MAP, parametersNodes, true)), true));
                        return new NodeTuple(representData(property.getName()),
                                new SequenceNode(Tag.SEQ, Arrays.<Node>asList(
                                        new MappingNode(Tag.MAP, Arrays.asList(classNameField, parametersField), false)
                                ), false));
                    }
                    return super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
                }
            }
        };
        representer.addClassTag(Config.class, Tag.MAP);
        Constructor constructor = new Constructor(Config.class);
        Yaml yaml = new Yaml(constructor, representer);
        Config config = yaml.loadAs(CassandraUnit.class.getResourceAsStream("/cassandra.prairie.yaml"), Config.class);
        config.commitlog_directory = createTmpDir("commitlog");
        config.data_file_directories = new String[]{createTmpDir("data")};
        config.hints_directory = createTmpDir("hints");
        config.cdc_raw_directory = createTmpDir("cdc_raw");
        config.saved_caches_directory = createTmpDir("saved_caches");
        config.native_transport_port = PortProvider.nextPort();
        config.listen_address = host;
        config.rpc_address = host;
        config.rpc_port = PortProvider.nextPort();
        config.seed_provider.parameters.put("seeds", host);
        File configFile = getTmpDir().resolve("cassandra.prairie.yml").toFile();
        try {
            yaml.dump(config, new FileWriter(configFile));
        } catch (IOException e) {
            throw new InitUnitException("Failed to dump config to file: " + configFile, e);
        }
        System.setProperty("cassandra.config", configFile.toURI().toString());
        System.setProperty("cassandra.start_native_transport", "true");
        return config;
    }

    private String createTmpDir(String name) {
        Path path = getTmpDir().resolve(name);
        path.toFile().mkdirs();
        return path.toAbsolutePath().toString();
    }
}
