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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by presidentio on 06.09.15.
 */
public class ByLineComparator<Entry> implements EntryComparator<Entry> {

    public CompareResponse<Entry> compare(List<Entry> expected, List<Entry> current) {
        List<Entry> expectedCopy = new ArrayList<Entry>(expected);
        List<Entry> unexpected = new ArrayList<Entry>();
        for (Entry s : current) {
            if (!expectedCopy.remove(s)) {
                unexpected.add(s);
            }
        }
        return new CompareResponse<Entry>(unexpected, expectedCopy);
    }

}
