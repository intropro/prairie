package com.intropro.prairie.unit.kafka;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.utils.EventChecker;
import com.intropro.prairie.utils.Waiter;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by presidentio on 10/15/15.
 */
@RunWith(BigDataTestRunner.class)
public class KafkaUnitTest {

    @BigDataUnit
    private KafkaUnit kafkaUnit;

    private ByLineComparator<String> byLineComparator = new ByLineComparator<>();

    @Test
    public void testKafkaUnit() throws Exception {
        String topic = "test_topic";
        KafkaProducer kafkaProducer = kafkaUnit.createProducer(topic);
        final List<String> resultMessages = new ArrayList<>();
        KafkaConsumer kafkaConsumer = kafkaUnit.createConsumer(topic, "test-group");
        kafkaConsumer.addListener(new MessageListener() {
            @Override
            public void consume(String message) {
                resultMessages.add(message);
            }
        });
        kafkaConsumer.start();
        final List<String> expectedMessages = IOUtils.readLines(KafkaUnitTest.class.getClassLoader().getResourceAsStream("messages.dat"));
        for (String expectedMessage : expectedMessages) {
            kafkaProducer.put(expectedMessage);
        }
        kafkaProducer.close();
        new Waiter(5000, new EventChecker() {
            @Override
            public boolean check() {
                return resultMessages.size() == expectedMessages.size();
            }
        }).await();
        kafkaConsumer.stop();
        byLineComparator.compare(expectedMessages, resultMessages).assertEquals();
    }
}