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
package com.intropro.prairie.comparator;

import java.util.List;

/**
 * Created by presidentio on 06.09.15.
 */
public class CompareResponse<Entry> {

    private List<Entry> unexpected;

    private List<Entry> missed;

    public CompareResponse() {
    }

    public CompareResponse(List<Entry> unexpected, List<Entry> missed) {
        this.unexpected = unexpected;
        this.missed = missed;
    }

    public List<Entry> getUnexpected() {
        return unexpected;
    }

    public CompareResponse setUnexpected(List<Entry> unexpected) {
        this.unexpected = unexpected;
        return this;
    }

    public List<Entry> getMissed() {
        return missed;
    }

    public CompareResponse setMissed(List<Entry> missed) {
        this.missed = missed;
        return this;
    }

    public void assertEquals() {
        String message = "";
        if (!getUnexpected().isEmpty()) {
            message += "\nUnexpected lines: " + getUnexpected();
        }
        if (!getMissed().isEmpty()) {
            message += "\nMissed lines:     " + getMissed();
        }
        assert getUnexpected().isEmpty() && getMissed().isEmpty() : message;
    }

    public void assertNotEquals() {
        assert  !getUnexpected().isEmpty() || !getMissed().isEmpty() : "No missed, no unexpected";
    }
}
