package com.intropro.prairie.unit.flume;

import org.apache.flume.instrumentation.util.JMXPollUtil;

import java.util.Map;

/**
 * Created by presidentio on 11/18/15.
 */
public class FlumeComponent {

    private String beaName;

    FlumeComponent(String beaName) {
        this.beaName = beaName;
    }

    public String getMetric(String name){
        Map<String, String> bean = JMXPollUtil.getAllMBeans().get(beaName);
        if (bean != null) {
            return bean.get(name);
        }
        return null;
    }

}
