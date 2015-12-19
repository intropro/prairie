package com.intropro.prairie.unit.flume;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.junit.PrairieRunner;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.utils.EventChecker;
import com.intropro.prairie.utils.FileUtils;
import com.intropro.prairie.utils.Waiter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by presidentio on 10/14/15.
 */
@RunWith(PrairieRunner.class)
public class FlumeUnitTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @PrairieUnit
    private FlumeUnit flumeUnit;

    private ByLineComparator<String> byLineComparator = new ByLineComparator<>();

    @Test
    public void testFlumeUnit() throws Exception {
        Properties properties = new Properties();
        properties.load(FlumeUnitTest.class.getClassLoader().getResourceAsStream("custom-source/flume.properties"));
        final String[] events = properties.getProperty("test_agent.sources.source.items").split(",");
        File outputFolder = folder.newFolder("test_agent");
        properties.setProperty("test_agent.sinks.sink.sink.directory", outputFolder.getAbsolutePath());
        final FlumeAgent flumeAgent = flumeUnit.createAgent("test_agent", properties);
        flumeAgent.start();
        new Waiter(5000, new EventChecker() {
            @Override
            public boolean check() {
                return flumeAgent.getSink("sink").processedEventCount() == events.length;
            }
        }).await();
        new Waiter(5000, new EventChecker() {
            @Override
            public boolean check() {
                String value = flumeAgent.getSource("source").getMetric(ConstSourceCounter.CUSTOM_COUNTER);
                return value != null && Integer.valueOf(value) == events.length;
            }
        }).await();
        flumeAgent.close();
        Assert.assertEquals("Unexpected event count processed", events.length, flumeAgent.getSink("sink").processedEventCount());
        Assert.assertEquals("1", flumeAgent.getSource("source").getMetric("Custom"));
        List<String> resultLines = FileUtils.readLineInDirectory(outputFolder);
        byLineComparator.compare(Arrays.asList(events), resultLines).assertEquals();
    }

}
