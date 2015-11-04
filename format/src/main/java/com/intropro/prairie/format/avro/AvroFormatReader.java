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

import com.intropro.prairie.format.AbstractFormatReader;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class AvroFormatReader extends AbstractFormatReader<Map<String, String>> {

    private InputStream inputStream;

    private DataFileReader<GenericRecord> dataFileReader;
    private DatumReader<GenericRecord> datumReader;
    private DataFileStream<GenericRecord> dataFileStream;

    private File tmpFile;

    AvroFormatReader(InputStream inputStream, Schema schema) {
        this.inputStream = inputStream;
        datumReader = new GenericDatumReader<>(schema);
    }

    @Override
    public Map<String, String> next() throws IOException {
        if (dataFileReader == null) {
            copyDataToTmpFile();
            dataFileStream = new DataFileStream<>(new FileInputStream(tmpFile), datumReader);
            dataFileReader = new DataFileReader<>(tmpFile, datumReader);
        }
        if(!dataFileStream.hasNext()){
            return null;
        }
        GenericRecord genericRecord = dataFileStream.next();
        Map<String, String> record = new HashMap<>(genericRecord.getSchema().getFields().size());
        for (Schema.Field field : genericRecord.getSchema().getFields()) {
            if (genericRecord.get(field.name()) != null) {
                record.put(field.name(), genericRecord.get(field.name()).toString());
            }
        }
        return record;
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
