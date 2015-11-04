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

import com.intropro.prairie.unit.common.BaseUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import org.apache.flume.node.Application;
import org.apache.flume.node.ConfigurationProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by presidentio on 10/13/15.
 */
public class FlumeUnit extends BaseUnit {

    private List<FlumeAgent> flumeAgents;

    public FlumeUnit() {
        super("flume");
    }

    @Override
    protected void init() throws InitUnitException {
        flumeAgents = new ArrayList<>();
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        for (FlumeAgent flumeAgent : flumeAgents) {
            try {
                flumeAgent.close();
            } catch (IOException e) {
                throw new DestroyUnitException(e);
            }
        }
    }

    public FlumeAgent createAgent(String agentName, Properties properties) {
        ConfigurationProvider configurationProvider =
                new PropertiesFlumeConfigurationProvider(
                        agentName, properties);
        Application application = new Application();
        application.handleConfigurationEvent(configurationProvider
                .getConfiguration());
        FlumeAgent flumeAgent = new FlumeAgent(application);
        flumeAgents.add(flumeAgent);
        return flumeAgent;
    }
}
