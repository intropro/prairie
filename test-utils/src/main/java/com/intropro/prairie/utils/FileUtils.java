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
package com.intropro.prairie.utils;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by presidentio on 10/14/15.
 */
public class FileUtils {
    
    public static List<String> readLineInDirectory(File file) throws IOException {
        List<String> result = new LinkedList<>();
        for (String child : file.list()) {
            File childFile = new File(file, child);
            if(childFile.isDirectory()){
                result.addAll(readLineInDirectory(childFile));
            }else{
                result.addAll(IOUtils.readLines(new FileInputStream(childFile)));
            }
        }
        return result;
    }
}
