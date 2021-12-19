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
package com.intropro.prairie.format.binary;

import com.intropro.prairie.format.AbstractFormatReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Created by presidentio on 10/7/15.
 */
public class BinaryFormatReader extends AbstractFormatReader<byte[]> {

    private static final int BUFFER_SIZE = 4096;

    private InputStream inputStream;

    BinaryFormatReader(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public byte[] next() throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int byteRead = inputStream.read(buffer);
        if(byteRead == -1){
            return null;
        }
        if(byteRead < BUFFER_SIZE){
            buffer = Arrays.copyOf(buffer, byteRead);
        }
        return buffer;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
