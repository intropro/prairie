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

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 10/15/15.
 */
public class KafkaConsumer implements MessageListener {

    private static Logger LOGGER = LogManager.getLogger(KafkaConsumer.class);

    private int numThreads = 1;

    private String topic;

    private ConsumerConnector consumer;
    private Properties properties;

    private ExecutorService executor;

    private List<MessageListener> messageConsumers = new ArrayList<>();

    KafkaConsumer(String topic, Properties properties) {
        this.topic = topic;
        this.properties = properties;
    }

    public void addListener(MessageListener messageListener) {
        messageConsumers.add(messageListener);
    }

    public void removeListener(MessageListener messageListener) {
        messageConsumers.remove(messageListener);
    }

    public void start() {
        LOGGER.info("Starting consumer on topic [" + topic + "] with [" + numThreads + "]");
        properties.setProperty("auto.offset.reset", "smallest");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(numThreads);
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.execute(new ConsumerTask(stream, this));
        }
        LOGGER.info("Started consumer on topic [" + topic + "]");
    }

    public void stop() {
        LOGGER.info("Stopping consumer on topic [" + topic + "] with [" + numThreads + "] threads");
        consumer.shutdown();
        executor.shutdownNow();
        while (!executor.isTerminated()) {
            try {
                if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
                LOGGER.info("Waiting to close consumer for topic [" + topic + "]");
            } catch (InterruptedException e) {
                LOGGER.info("Thread interrupted", e);
            }
        }
        LOGGER.info("Stopped consumer on topic [" + topic + "]");
    }

    @Override
    public void consume(String entity) {
        for (MessageListener messageConsumer : messageConsumers) {
            messageConsumer.consume(entity);
        }
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

}
