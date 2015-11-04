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

import org.apache.flume.instrumentation.util.JMXPollUtil;

/**
 * Created by presidentio on 10/14/15.
 */
public class Channel {

    private static final String EVENT_PUT_SUCCESS_COUNT = "EventPutSuccessCount";
    private static final String EVENT_TAKE_SUCCESS_COUNT = "EventTakeSuccessCount";

    private String name;

    Channel(String name) {
        this.name = name;
    }

    public int eventPutCount(){
        String eventCount = JMXPollUtil.getAllMBeans().get("CHANNEL." + name).get(EVENT_PUT_SUCCESS_COUNT);
        return eventCount == null ? 0 : Integer.valueOf(eventCount);
    }

    public int eventTakeCount(){
        String eventCount = JMXPollUtil.getAllMBeans().get("CHANNEL." + name).get(EVENT_TAKE_SUCCESS_COUNT);
        return eventCount == null ? 0 : Integer.valueOf(eventCount);
    }

}
