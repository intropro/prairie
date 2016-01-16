package com.intropro.prairie.utils;

import java.util.Map;

/**
 * Created by presidentio on 1/10/16.
 */
public class StringUtils {

    public static String replacePlaceholders(String script, Map<String, String> configurations) {
        for (Map.Entry<String, String> configuration : configurations.entrySet()) {
            script = script.replaceAll("\\$\\{" + configuration.getKey() + "\\}", configuration.getValue());
        }
        return script;
    }

}
