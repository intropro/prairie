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
package com.intropro.prairie.unit.oozie.exception;

import com.intropro.prairie.unit.common.exception.BigDataTestFrameworkException;

/**
 * Created by presidentio on 9/18/15.
 */
public class OozieException extends BigDataTestFrameworkException {
    public OozieException() {
        super();
    }

    public OozieException(String message) {
        super(message);
    }

    public OozieException(String message, Throwable cause) {
        super(message, cause);
    }

    public OozieException(Throwable cause) {
        super(cause);
    }
}
