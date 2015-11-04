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
package com.intropro.prairie.format.seq;

import com.intropro.prairie.format.AbstractFormatReader;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.*;

/**
 * Created by presidentio on 10/11/15.
 */
public class SequenceFormatReader extends AbstractFormatReader<String> {

    private InputStream inputStream;

    private File tmpFile;

    private SequenceFile.Reader reader;

    private NullWritable key = NullWritable.get();
    private Text value = new Text();

    SequenceFormatReader(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public String next() throws IOException {
        if (reader == null) {
            copyDataToTmpFile();
            Configuration configuration = new Configuration();
            reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(new Path(tmpFile.toURI())));
        }
        reader.next(key, value);
        return value.toString();
    }

    @Override
    public void close() throws IOException {
        reader.close();
        tmpFile.delete();
    }

    private void copyDataToTmpFile() throws IOException {
        tmpFile = File.createTempFile("seq-", ".dat");
        OutputStream outputStream = new FileOutputStream(tmpFile);
        IOUtils.copy(inputStream, outputStream);
        outputStream.close();
        inputStream.close();
    }
}
