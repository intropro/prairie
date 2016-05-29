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

import com.intropro.prairie.format.AbstractFormatReader;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class AvroFormatReader extends AbstractFormatReader<Map<String, Object>> {

    private InputStream inputStream;

    private DataFileReader<GenericRecord> dataFileReader;
    private DatumReader<GenericRecord> datumReader;
    private DataFileStream<GenericRecord> dataFileStream;

    private File tmpFile;

    AvroFormatReader(InputStream inputStream, Schema schema) {
        this.inputStream = inputStream;
        datumReader = schema == null ? new GenericDatumReader<GenericRecord>()
                : new GenericDatumReader<GenericRecord>(schema);
    }

    @Override
    public Map<String, Object> next() throws IOException {
        if (dataFileReader == null) {
            copyDataToTmpFile();
            dataFileStream = new DataFileStream<>(new FileInputStream(tmpFile), datumReader);
            dataFileReader = new DataFileReader<>(tmpFile, datumReader);
        }
        if (!dataFileStream.hasNext()) {
            return null;
        }
        GenericRecord genericRecord = dataFileStream.next();
        return parseRecord(genericRecord, genericRecord.getSchema());
    }

    private Object parseObject(Object object, Schema schema) throws IOException {
        switch (schema.getType()) {
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case NULL:
                return object;
            case STRING:
                return object.toString();
            case RECORD:
                return parseRecord((GenericRecord) object, schema);
            case ARRAY:
                return parseList((List<Object>) object, schema);
            case MAP:
                return parseMap((Map<Object, Object>) object, schema);
            case UNION:
                int typePosition = GenericData.get().resolveUnion(schema, object);
                return parseObject(object, schema.getTypes().get(typePosition));
            default:
                throw new IOException("Type does not supported yes: " + schema.getType());
        }
    }

    private Map<String, Object> parseRecord(GenericRecord record, Schema schema) throws IOException {
        Map<String, Object> parsed = new HashMap<>(schema.getFields().size());
        for (Schema.Field field : schema.getFields()) {
            parsed.put(field.name(), parseObject(record.get(field.name()), field.schema()));
        }
        return parsed;
    }

    private List<Object> parseList(List<Object> originItems, Schema schema) throws IOException {
        List<Object> parsedItems = new ArrayList<>(originItems.size());
        for (Object item : originItems) {
            parsedItems.add(parseObject(item, schema.getElementType()));
        }
        return parsedItems;
    }

    private Map<String, Object> parseMap(Map<Object, Object> originMap, Schema schema) throws IOException {
        Map<String, Object> parsedMap = new HashMap<>(originMap.size());
        for (Map.Entry<Object, Object> entry : originMap.entrySet()) {
            parsedMap.put(entry.getKey().toString(), parseObject(entry.getValue(), schema.getValueType()));
        }
        return parsedMap;
    }

    @Override
    public void close() throws IOException {
        tmpFile.delete();
    }

    private void copyDataToTmpFile() throws IOException {
        tmpFile = File.createTempFile("avro-", ".dat");
        OutputStream outputStream = new FileOutputStream(tmpFile);
        IOUtils.copy(inputStream, outputStream);
        outputStream.close();
        inputStream.close();
    }
}
