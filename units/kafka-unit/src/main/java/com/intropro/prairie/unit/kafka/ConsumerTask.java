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

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Vitalii_Gergel on 2/10/2015.
 */
public class ConsumerTask implements Runnable {
    
    private static Logger LOGGER = LogManager.getLogger(ConsumerTask.class);
    private KafkaStream<byte[], byte[]> stream;
    private MessageListener messageConsumer;

    public ConsumerTask(KafkaStream<byte[], byte[]> stream, MessageListener messageConsumer) {
        this.stream = stream;
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        LOGGER.info("Start consuming messages in thread: " + Thread.currentThread().getName());
        while (it.hasNext()) {
            String message = new String(it.next().message());
            LOGGER.debug("Consume message: " + message);
            messageConsumer.consume(message);
        }
    }
}
