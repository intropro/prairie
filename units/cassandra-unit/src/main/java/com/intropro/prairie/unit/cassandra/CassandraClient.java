package com.intropro.prairie.unit.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.net.InetSocketAddress;
import java.util.Collections;

/**
 * Created by presidentio on 10/4/16.
 */
public class CassandraClient {

    private String host;
    private int port;
    private Cluster cluster;

    public CassandraClient(String host, int port) {
        this.host = host;
        this.port = port;
        cluster = Cluster.builder()
                .addContactPointsWithPorts(Collections.singleton(new InetSocketAddress(host, port)))
                .build();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Session getSession() {
        return cluster.connect();
    }

}
