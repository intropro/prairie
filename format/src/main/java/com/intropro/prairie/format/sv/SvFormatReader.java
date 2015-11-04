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

import com.intropro.prairie.format.AbstractFormatReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by presidentio on 10/7/15.
 */
public class SvFormatReader extends AbstractFormatReader<Map<String, String>> {

    private InputStreamReader inputStream;
    private CSVParser csvParser;
    private char delimiter;
    private Iterator<CSVRecord> iterator;

    SvFormatReader(InputStream inputStream, char delimiter) {
        this.inputStream = new InputStreamReader(inputStream);
        this.delimiter = delimiter;
    }

    @Override
    public Map<String, String> next() throws IOException {
        if (csvParser == null) {
            csvParser = CSVFormat.DEFAULT.withHeader().withDelimiter(delimiter).parse(inputStream);
            iterator = csvParser.iterator();
        }
        if (!iterator.hasNext()) {
            return null;
        }
        CSVRecord csvRecord = iterator.next();
        return csvRecord.toMap();
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
