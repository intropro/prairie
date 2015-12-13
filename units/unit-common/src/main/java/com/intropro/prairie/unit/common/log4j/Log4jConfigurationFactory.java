package com.intropro.prairie.unit.common.log4j;

import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.xml.XmlConfigurationFactory;

/**
 * Created by presidentio on 12/13/15.
 */
@Plugin(
        name = "Log4jConfigurationFactory",
        category = "ConfigurationFactory"
)
@Order(4)
public class Log4jConfigurationFactory extends XmlConfigurationFactory {

    public static final String[] SUFFIXES = new String[] {".prairie.xml"};

    @Override
    public String[] getSupportedTypes() {
        return SUFFIXES;
    }
}
