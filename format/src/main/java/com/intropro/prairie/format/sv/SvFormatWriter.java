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
package com.intropro.prairie.format.sv;

import com.intropro.prairie.format.OutputFormatWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class SvFormatWriter implements OutputFormatWriter<Map<String, Object>> {

    private Writer writer;
    private CSVPrinter csvPrinter;
    private char delimiter;
    private String[] headers;

    SvFormatWriter(OutputStream outputStream, char delimiter) {
        this.writer = new OutputStreamWriter(outputStream);
        this.delimiter = delimiter;
    }

    @Override
    public void write(Map<String, Object> line) throws IOException {
        if (csvPrinter == null) {
            headers = line.keySet().toArray(new String[line.size()]);
            csvPrinter = CSVFormat.DEFAULT.withDelimiter(delimiter).withHeader(headers).print(writer);
        }
        Object[] values = new Object[headers.length];
        for (int i = 0; i < headers.length; i++) {
            values[i] = line.get(headers[i]);
        }
        csvPrinter.printRecord(values);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

}
