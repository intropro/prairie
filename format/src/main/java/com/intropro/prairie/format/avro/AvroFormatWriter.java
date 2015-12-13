/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intropro.prairie.format.avro;

import com.intropro.prairie.format.OutputFormatWriter;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class AvroFormatWriter implements OutputFormatWriter<Map<String, Object>> {

    private OutputStream outputStream;

    private Schema schema;
    private boolean validate;

    private DatumWriter<GenericRecord> datumWriter;
    private DataFileWriter<GenericRecord> dataFileWriter;

    AvroFormatWriter(OutputStream outputStream, Schema schema) {
        this.outputStream = outputStream;
        this.schema = schema;
        datumWriter = new GenericDatumWriter<>(schema);
    }

    @Override
    public void write(Map<String, Object> object) throws IOException {
        if (dataFileWriter == null) {
            dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, outputStream);
        }
        dataFileWriter.append(transformToRecord(object, schema));
    }

    private Object transform(Object object, Schema schema) throws IOException {
        switch (schema.getType()) {
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case NULL:
            case STRING:
                return object;
            case RECORD:
                return transformToRecord((Map<String, Object>) object, schema);
            case ARRAY:
                return transformToList((List<Object>) object, schema);
            case MAP:
                return transformToMap((Map<String, Object>) object, schema);
            default:
                throw new IOException("Type does not supported yes: " + schema.getType());
        }
    }

    private GenericRecord transformToRecord(Map<String, Object> object, Schema schema) throws IOException {
        GenericRecord record = new GenericData.Record(schema);
        if (validate && object.size() != schema.getFields().size()) {
            throw new AvroTypeException("Field count mismatch, expected: " + schema.getFields().size()
                    + " actual: " + object.size());
        }
        for (Schema.Field field : schema.getFields()) {
            if (object.containsKey(field.name())) {
                record.put(field.name(), transform(object.get(field.name()), field.schema()));
            } else {
                if (validate) {
                    throw new AvroTypeException("Field does not found: " + field.name());
                }
            }
        }
        return record;
    }

    private Object transformToList(List<Object> items, Schema schema) throws IOException {
        List<Object> transformedItems = new ArrayList<>(items.size());
        for (Object item : items) {
            transformedItems.add(transform(item, schema.getElementType()));
        }
        return transformedItems;
    }

    private Object transformToMap(Map<String, Object> items, Schema schema) throws IOException {
        Map<Object, Object> transformedMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : items.entrySet()) {
            transformedMap.put(entry.getKey(), transform(entry.getValue(), schema.getValueType()));
        }
        return transformedMap;
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
    }

    public boolean isValidate() {
        return validate;
    }

    public AvroFormatWriter setValidate(boolean validate) {
        this.validate = validate;
        return this;
    }
}
