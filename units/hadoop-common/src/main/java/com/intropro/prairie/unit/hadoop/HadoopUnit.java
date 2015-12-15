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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by presidentio on 9/18/15.
 */
public abstract class HadoopUnit extends BaseUnit {

    public HadoopUnit(String unitName) {
        super(unitName);
    }

    protected Configuration createConfig() {
        Configuration conf = new Configuration();
        conf.addResource("prairie-site.xml");
        conf.addResource("core-site.prairie.xml");
        conf.set("hadoop.tmp.dir", getTmpDir().toString());
        return conf;
    }

    public File dumpConfigs() throws IOException {
        File confFile = new File(getConfDir(), getUnitName() + "-site.xml");
        FileWriter configWriter = new FileWriter(confFile);
        getConfig().writeXml(configWriter);
        configWriter.close();
        return confFile;
    }

    private File getConfDir(){
        File confDir = new File(getTmpDir().toFile(), "conf");
        confDir.mkdirs();
        return confDir;
    }

    public abstract Configuration getConfig();
}
