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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.Properties;

/**
 * Created by presidentio on 10/15/15.
 */
public class KafkaProducer implements Closeable {

    private static Logger LOGGER = LogManager.getLogger(KafkaProducer.class);

    private String topic;

    private org.apache.kafka.clients.producer.Producer<String, String> producer;

    KafkaProducer(String topic, Properties configs) {
        this.topic = topic;
        String bootstrapServers = configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString();
        LOGGER.info("Creating new kafka producer for bootstrap servers [" + bootstrapServers + "]");
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(configs, new StringSerializer(),
                new StringSerializer());
    }

    public void put(String entity) {
        LOGGER.info("Produce message: " + entity);
        producer.send(new ProducerRecord<String, String>(topic, entity));
    }

    @Override
    public void close() {
        LOGGER.info("Close produce with topic [" + topic + "]");
        producer.close();
    }
}
