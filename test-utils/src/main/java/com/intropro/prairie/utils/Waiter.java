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
package com.intropro.prairie.utils;

/**
 * Created by presidentio on 10/14/15.
 */
public class Waiter {

    private long time;

    private EventChecker eventChecker;

    private long interval = 100;

    public Waiter(long time, EventChecker eventChecker) {
        this.time = time;
        this.eventChecker = eventChecker;
    }

    public boolean await() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (startTime + time > System.currentTimeMillis()) {
            if (eventChecker.check()) {
                return true;
            }
            Thread.sleep(interval);
        }
        return false;
    }

    public long getTime() {
        return time;
    }

    public Waiter setTime(long time) {
        this.time = time;
        return this;
    }

    public EventChecker getEventChecker() {
        return eventChecker;
    }

    public Waiter setEventChecker(EventChecker eventChecker) {
        this.eventChecker = eventChecker;
        return this;
    }

    public long getInterval() {
        return interval;
    }

    public Waiter setInterval(long interval) {
        this.interval = interval;
        return this;
    }
}
