package com.intropro.prairie.comparator.cline;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.CompareResponse;
import com.intropro.prairie.format.Format;
import com.intropro.prairie.format.InputFormatReader;
import com.intropro.prairie.format.avro.AvroFormat;
import com.intropro.prairie.format.exception.FormatException;
import com.intropro.prairie.format.json.JsonFormat;
import com.intropro.prairie.format.sv.SvFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Ihor Zaitsev on 29.08.2016.
 */
public class CommandLineComparator {

    private final ByLineComparator byLineComparator = new ByLineComparator();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        CommandLineComparator clc = new CommandLineComparator();
        int result = clc.compare(args);
        System.exit(result);
    }

    public int compare(String[] params){
        FileInfo firstFileInfo = getFileInfo(params,0);
        FileInfo secondFileInfo = getFileInfo(params, FileType.SV == firstFileInfo.fileType ? 3 : 2);
        return compare(firstFileInfo,secondFileInfo);
    }

    public int compare(FileInfo firstFile, FileInfo secondFile){
        List firstLines = firstFile.isHadoop ? readLinesFromHadoop(firstFile): readLinesFromFS(firstFile);
        List secondLines = secondFile.isHadoop ? readLinesFromHadoop(secondFile): readLinesFromFS(secondFile);

        CompareResponse response = byLineComparator.compare(firstLines, secondLines);

        List<Map<String,Object>> missed = response.getMissed();
        List<Map<String,Object>> unexpected = response.getUnexpected();

        if(missed.isEmpty()){
            System.out.println("No missed lines found");
        } else {
            System.out.println("Missed lines:");
            printLines(missed);
        }

        System.out.println("--------------------------------------------------------------------------------------------");

        if(unexpected.isEmpty()){
            System.out.println("No unexpected lines found");
        } else {
            System.out.println("Unexpected lines:");
            printLines(unexpected);
        }

        return missed.size()+unexpected.size();
    }

    private void printLines(List<Map<String, Object>> missed) {
        for(Map<String,Object> line : missed){
            try {
                System.out.println(objectMapper.writeValueAsString(line));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private FileInfo getFileInfo(String[] params, int startIndex) {
        String path = params[startIndex++];

        FileType fileType = null;
        try {
            fileType = FileType.valueOf(params[startIndex++].toUpperCase());
        } catch (IllegalArgumentException e){
            System.out.println("Format "+params[--startIndex]+" is not supported");
            System.exit(1);
        }

        Format format = null;
        if(fileType == FileType.SV){
            if(params[startIndex].length() == 1) {
                format = new SvFormat(params[startIndex].charAt(0));
            } else {
                System.out.println("Wrong delimiter for Sv format - '"+params[startIndex]+"'");
                System.exit(1);
            }
        } else {
            format = FileType.AVRO == fileType ? new AvroFormat() : new JsonFormat();
        }

        return new FileInfo(path, fileType, format);
    }

    private List readLinesFromFS(FileInfo file) {
        List lines = new ArrayList();
        Iterator<File> iterator = null;
        try {
            iterator = FileUtils.iterateFiles(Paths.get(file.path).toFile(), null,  true);
        }catch (IllegalArgumentException e){
            System.out.println("Path '"+file.path+"' is invalid");
            System.exit(1);
        }
        while (iterator.hasNext()){
            File tempFile = iterator.next();
            if(isCorrectFileName(tempFile.getName())){
                try(InputStream in = Files.newInputStream(tempFile.toPath()); InputFormatReader reader = file.format.createReader(in)){
                    lines.addAll(reader.all());
                } catch (FormatException | IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return lines;
    }

    private List readLinesFromHadoop(FileInfo file){
        List fileList = new ArrayList<>();
        try (FileSystem fileSystem = FileSystem.get(new URI(file.path), getConfiguration())){
            RemoteIterator<LocatedFileStatus> filesIterator = fileSystem.listFiles(new org.apache.hadoop.fs.Path(file.path), true);
            while (filesIterator.hasNext()){
                LocatedFileStatus f = filesIterator.next();
                final String fileName = f.getPath().getName();
                System.out.println(f.getPath());
                if(isCorrectFileName(fileName)) {
                    try (InputStream in = fileSystem.open(f.getPath()); InputFormatReader reader = file.format.createReader(in)) {
                        fileList.add(reader.all());
                    } catch (FormatException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (FileNotFoundException e){
            System.out.println("Wrong hadoop path - '"+file.path+"'");
            System.exit(1);
        } catch (IOException e) {
            System.out.println("HDFS connection error - "+e.getMessage());
            System.exit(1);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fileList;
    }

    private boolean isCorrectFileName(String fileName) {
        return !fileName.startsWith("_") && !fileName.startsWith(".");
    }

    public Configuration getConfiguration(){
        return new Configuration();
    }

    private class FileInfo {
        final String path;
        final FileType fileType;
        final Format format;
        final boolean isHadoop;

        public FileInfo(String path, FileType fileType, Format format) {
            this.path = path;
            this.fileType = fileType;
            this.format = format;
            isHadoop = path.toLowerCase().startsWith("hdfs:");
        }
    }

    private enum FileType {
        JSON, AVRO, SV
    }

}
