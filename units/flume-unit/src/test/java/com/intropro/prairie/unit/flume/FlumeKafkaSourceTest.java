package com.intropro.prairie.unit.flume;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.junit.BigDataTestRunner;
import com.intropro.prairie.unit.common.annotation.BigDataUnit;
import com.intropro.prairie.unit.kafka.KafkaProducer;
import com.intropro.prairie.unit.kafka.KafkaUnit;
import com.intropro.prairie.utils.EventChecker;
import com.intropro.prairie.utils.FileUtils;
import com.intropro.prairie.utils.Waiter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.*;

/**
 * Created by presidentio on 11/4/15.
 */
@RunWith(BigDataTestRunner.class)
public class FlumeKafkaSourceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BigDataUnit
    private FlumeUnit flumeUnit;
    @BigDataUnit
    private KafkaUnit kafkaUnit;
    private ByLineComparator<String> byLineComparator = new ByLineComparator<>();

    private String topic = "test_topic";
    private int messageCount = 10;

    @Test
    public void testFlumeUnitKafkaSource() throws Exception {
        Properties properties = new Properties();
        properties.load(FlumeUnitTest.class.getClassLoader().getResourceAsStream("kafka-source/flume.properties"));
        properties.setProperty("test_agent.sources.kafka-source.zookeeperConnect", kafkaUnit.getZookeeperConnect());
        properties.setProperty("test_agent.sources.kafka-source.topic", topic);
        File outputFolder = folder.newFolder("test_agent");
        properties.setProperty("test_agent.sinks.sink.sink.directory", outputFolder.getAbsolutePath());
        final FlumeAgent flumeAgent = flumeUnit.createAgent("test_agent", properties);
        flumeAgent.start();
        KafkaProducer kafkaProducer = kafkaUnit.createProducer(topic);
        final List<String> messages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            String message = UUID.randomUUID().toString();
            messages.add(message);
            kafkaProducer.put(message);
        }
        new Waiter(5000, new EventChecker() {
            @Override
            public boolean check() {
                return flumeAgent.getSink("sink").processedEventCount() == messages.size();
            }
        }).await();
        Assert.assertEquals("Unexpected event count processed", messages.size(), flumeAgent.getSink("sink").processedEventCount());
        flumeAgent.close();
        List<String> resultLines = FileUtils.readLineInDirectory(outputFolder);
        byLineComparator.compare(messages, resultLines).assertEquals();
    }
}
