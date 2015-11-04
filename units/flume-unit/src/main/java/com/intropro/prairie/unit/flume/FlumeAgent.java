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

import org.apache.flume.node.Application;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by presidentio on 10/13/15.
 */
public class FlumeAgent implements Closeable {

    private Application application;

    public FlumeAgent(Application application) {
        this.application = application;
    }

    public void start() {
        application.start();
    }

    @Override
    public void close() throws IOException {
        application.stop();
    }

    public Source getSource(String name) {
        return new Source(name);
    }

    public Channel getChannel(String name) {
        return new Channel(name);
    }

    public Sink getSink(String name) {
        return new Sink(name);
    }
}
