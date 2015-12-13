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
package com.intropro.prairie.format.json;

import com.intropro.prairie.format.OutputFormatWriter;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class JsonFormatWriter implements OutputFormatWriter<Map<String, Object>> {

    private OutputStream outputStream;

    private ObjectMapper objectMapper = new ObjectMapper();

    private boolean firstLine = true;

    JsonFormatWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(Map<String, Object> line) throws IOException {
        String lineStr = objectMapper.writeValueAsString(line);
        if (!firstLine) {
            outputStream.write('\n');
        }
        outputStream.write(lineStr.getBytes());
        firstLine = false;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
