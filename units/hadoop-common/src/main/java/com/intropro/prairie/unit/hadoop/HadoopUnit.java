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
package com.intropro.prairie.unit.hadoop;

import com.intropro.prairie.unit.common.BaseUnit;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by presidentio on 9/18/15.
 */
public abstract class HadoopUnit extends BaseUnit {

    public HadoopUnit(String unitName) {
        super(unitName);
    }

    protected Configuration gatherConfigs() {
        Configuration conf = new Configuration();
        conf.addResource("core-site.prairie.xml");
        conf.set("hadoop.tmp.dir", getTmpDir().toString());
        conf.addResource("prairie-site.xml");
        return conf;
    }

    public abstract Configuration getConfig();
}
