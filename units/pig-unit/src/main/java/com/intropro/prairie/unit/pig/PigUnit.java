/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intropro.prairie.unit.pig;

import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import com.intropro.prairie.unit.hdfs.HdfsUnit;
import com.intropro.prairie.unit.yarn.YarnUnit;
import com.intropro.prairie.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by presidentio on 10/21/15.
 */
public class PigUnit extends HadoopUnit {

    @PrairieUnit
    private HdfsUnit hdfsUnit;

    @PrairieUnit
    private YarnUnit yarnUnit;

    private PigServer pigServer;

    public PigUnit() {
        super("pig");
    }

    @Override
    public Configuration gatherConfigs() {
        Configuration configuration = new Configuration(super.gatherConfigs());
        Iterator<Map.Entry<String, String>> iterator = yarnUnit.getConfig().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            configuration.set(next.getKey(), next.getValue());
        }
        configuration.addResource("pig-site.prairie.xml");
        return configuration;
    }

    @Override
    public Configuration getConfig() {
        return gatherConfigs();
    }

    @Override
    protected void init() throws InitUnitException {
        try {
            Configuration configuration = gatherConfigs();
            PigContext pigContext = new PigContext(ExecType.MAPREDUCE, configuration);
            pigServer = new PigServer(pigContext);
        } catch (PigException e) {
            throw new InitUnitException(e);
        }
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        pigServer.shutdown();
    }

    public void run(String script, Map<String, String> placeholders) throws PigException {
        run(StringUtils.replacePlaceholders(script, placeholders));
    }

    public void run(String script) throws PigException {
        try {
            pigServer.registerQuery(script);
        } catch (IOException e) {
            throw new PigException(e);
        }
    }
}
