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
package com.intropro.prairie.format.avro;

import com.intropro.prairie.format.Format;
import com.intropro.prairie.format.InputFormatReader;
import com.intropro.prairie.format.OutputFormatWriter;
import org.apache.avro.Schema;

import java.io.*;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class AvroFormat implements Format<Map<String, String>> {

    private Schema schema;

    public AvroFormat(String schemaStr) {
        this.schema = new Schema.Parser().parse(schemaStr);
    }

    public AvroFormat(InputStream inputStream) throws IOException {
        this.schema = new Schema.Parser().parse(inputStream);
    }

    public AvroFormat(File file) throws IOException {
        this.schema = new Schema.Parser().parse(file);
    }

    @Override
    public InputFormatReader<Map<String, String>> createReader(InputStream inputStream) {
        return new AvroFormatReader(inputStream, schema);
    }

    @Override
    public OutputFormatWriter<Map<String, String>> createWriter(OutputStream outputStream) {
        return new AvroFormatWriter(outputStream, schema);
    }

}
