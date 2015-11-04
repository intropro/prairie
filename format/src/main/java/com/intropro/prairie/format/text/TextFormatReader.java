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
package com.intropro.prairie.format.text;

import com.intropro.prairie.format.AbstractFormatReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by presidentio on 10/7/15.
 */
public class TextFormatReader extends AbstractFormatReader<String> {

    private BufferedReader bufferedReader;

    TextFormatReader(InputStream inputStream) {
        this.bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    }

    @Override
    public String next() throws IOException {
        String line = bufferedReader.readLine();
        return line;
    }

    @Override
    public void close() throws IOException {
        bufferedReader.close();
    }
}
