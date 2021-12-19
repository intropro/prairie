package com.intropro.prairie.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by presidentio on 8/18/16.
 */
public class JarUtils {

    public static File findJar(String nameRegexp){
        List<File> jars = getFiles(System.getProperty("java.class.path"));
        for (File jar : jars) {
            if(jar.getName().matches(nameRegexp)){
                return jar;
            }
        }
        return null;
    }

    private static List<File> getFiles(String paths) {
        List<File> filesList = new ArrayList<>();
        for (final String path : paths.split(File.pathSeparator)) {
            final File file = new File(path);
            if( file.isDirectory()) {
                recurse(filesList, file);
            }
            else {
                filesList.add(file);
            }
        }
        return filesList;
    }

    private static void recurse(List<File> filesList, File f) {
        File list[] = f.listFiles();
        for (File file : list) {
            if (file.isDirectory()) {
                recurse(filesList, file);
            }
            else {
                filesList.add(file);
            }
        }
    }



}
