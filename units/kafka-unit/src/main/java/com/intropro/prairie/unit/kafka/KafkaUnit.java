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
package com.intropro.prairie.unit.kafka;

import com.intropro.prairie.unit.common.BaseUnit;
import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.zookeeper.ZookeeperUnit;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by presidentio on 10/14/15.
 */
public class KafkaUnit extends BaseUnit {

    private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    private static final String BROKER_ID = "broker.id";
    private static final String LOG_DIRS = "log.dirs";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String GROUP_ID = "group.id";

    private String host = "localhost";
    private int port;

    public KafkaServerStartable kafka;

    private Properties properties;

    @BigDataUnit
    private ZookeeperUnit zookeeperUnit;

    public KafkaUnit() {
        super("kafka");
    }

    @Override
    protected void init() throws InitUnitException {
        port = PortProvider.nextPort();
        properties = new Properties();
        properties.put(HOST, host);
        properties.put(PORT, "" + port);
        properties.put(BROKER_ID, "1");
        properties.put(ZOOKEEPER_CONNECT, getZookeeperConnect());
        properties.put(LOG_DIRS, getTmpDir().toString() + "/logs");
        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        kafka = new KafkaServerStartable(kafkaConfig);
        kafka.startup();
    }

    public KafkaProducer createProducer(String topicName) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList());
        return new KafkaProducer(topicName, properties);
    }

    public KafkaConsumer createConsumer(String topicName, String groupId) {
        Properties properties = new Properties();
        properties.put(GROUP_ID, groupId);
        properties.put(ZOOKEEPER_CONNECT, getZookeeperConnect());
        return new KafkaConsumer(topicName, properties);
    }

    public String getZookeeperConnect(){
        return zookeeperUnit.getHost() + ":" + zookeeperUnit.getPort();
    }

    public String getBrokerList(){
        return host + ":" + port;
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        kafka.shutdown();
        kafka.awaitShutdown();
    }
}
