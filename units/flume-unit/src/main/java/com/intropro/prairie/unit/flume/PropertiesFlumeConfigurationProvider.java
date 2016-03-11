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
package com.intropro.prairie.unit.flume;

import com.google.common.collect.Maps;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.node.AbstractConfigurationProvider;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by presidentio on 10/13/15.
 */
public class PropertiesFlumeConfigurationProvider extends AbstractConfigurationProvider {

    private Properties properties;

    public PropertiesFlumeConfigurationProvider(String agentName, Properties properties) {
        super(agentName);
        this.properties = properties;
    }

    @Override
    protected FlumeConfiguration getFlumeConfiguration() {
        return new FlumeConfiguration(toMap(properties));
    }


    protected Map<String, String> toMap(Properties properties) {
        HashMap result = Maps.newHashMap();
        Enumeration propertyNames = properties.propertyNames();

        while(propertyNames.hasMoreElements()) {
            String name = (String)propertyNames.nextElement();
            String value = properties.getProperty(name);
            result.put(name, value);
        }

        return result;
    }
}
