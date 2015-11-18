package com.intropro.prairie.unit.flume;

import org.apache.flume.instrumentation.SourceCounter;

/**
 * Created by presidentio on 11/18/15.
 */
public class ConstSourceCounter extends SourceCounter implements ConstSourceCounterMBean {

    public static final String CUSTOM_COUNTER = "source.custom";

    private static final String[] ATTRIBUTES = new String[]{CUSTOM_COUNTER};

    public ConstSourceCounter(String name) {
        super(name, ATTRIBUTES);
    }

    public void incrementCustom(){
        increment(CUSTOM_COUNTER);
    }

    @Override
    public long getCustom(){
        return get(CUSTOM_COUNTER);
    }

}
