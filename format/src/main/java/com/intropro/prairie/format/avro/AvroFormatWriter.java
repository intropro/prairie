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

import com.intropro.prairie.format.OutputFormatWriter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class AvroFormatWriter implements OutputFormatWriter<Map<String, String>> {

    private OutputStream outputStream;

    private Schema schema;

    private DatumWriter<GenericRecord> datumWriter;
    private DataFileWriter<GenericRecord> dataFileWriter;

    AvroFormatWriter(OutputStream outputStream, Schema schema) {
        this.outputStream = outputStream;
        this.schema = schema;
        datumWriter = new GenericDatumWriter<>(schema);
    }

    @Override
    public void write(Map<String, String> line) throws IOException {
        if (dataFileWriter == null) {
            dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, outputStream);
        }
        GenericRecord row = new GenericData.Record(schema);
        for (Map.Entry<String, String> stringStringEntry : line.entrySet()) {
            row.put(stringStringEntry.getKey(), stringStringEntry.getValue());
        }
        dataFileWriter.append(row);
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
    }
}
