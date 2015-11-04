package com.intropro.prairie.unit.oozie.java;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by presidentio on 9/15/15.
 */
public class OozieTestActionClass {

    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        file.getParentFile().mkdirs();
        FileWriter fileWriter = new FileWriter(file);
        IOUtils.write(args[1], fileWriter);
        fileWriter.close();
    }
}
