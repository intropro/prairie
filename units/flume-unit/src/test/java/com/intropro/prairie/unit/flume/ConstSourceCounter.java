package com.intropro.prairie.unit.flume;

import org.apache.flume.instrumentation.SourceCounter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by presidentio on 11/18/15.
 */
public class ConstSourceCounter extends SourceCounter implements ConstSourceCounterMBean {

    public static final String CUSTOM_COUNTER = "source.custom";

    private final Map<String, AtomicLong> counterMap;

    private static final String[] ATTRIBUTES = new String[]{CUSTOM_COUNTER};

    public ConstSourceCounter(String name) {
        super(name);
        counterMap = new HashMap<>();
        for (String attribute : ATTRIBUTES) {
            counterMap.put(attribute, new AtomicLong(0L));
        }
    }

    public void incrementCustom(){
        counterMap.get(CUSTOM_COUNTER).incrementAndGet();
    }

    @Override
    public long getCustom(){
        return counterMap.get(CUSTOM_COUNTER).get();
    }

}
