/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intropro.prairie.unit.hdfs;

import com.intropro.prairie.comparator.ByLineComparator;
import com.intropro.prairie.comparator.CompareResponse;
import com.intropro.prairie.format.Format;
import com.intropro.prairie.format.InputFormatReader;
import com.intropro.prairie.format.OutputFormatWriter;
import com.intropro.prairie.format.exception.FormatException;
import com.intropro.prairie.unit.common.Version;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.hadoop.HadoopUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Properties;

/**
 * Created by presidentio on 03.09.15.
 */
@PrairieUnit
public class HdfsUnit extends HadoopUnit {

    private static final Logger LOGGER = LogManager.getLogger(HdfsUnit.class);

    public static final Version VERSION = getVersion();

    private MiniDFSCluster miniDFSCluster;

    private ByLineComparator byLineComparator = new ByLineComparator();

    private String user = System.getProperty("user.name");

    public HdfsUnit() {
        super("hdfs");
    }

    @Override
    public void init() throws InitUnitException {
        try {
            miniDFSCluster = new MiniDFSCluster.Builder(gatherConfigs()).build();
            miniDFSCluster.waitClusterUp();
            createHomeDirectory();
        } catch (IOException e) {
            throw new InitUnitException("Failed to start hdfs", e);
        }
    }

    @Override
    protected Configuration gatherConfigs() {
        Configuration conf = super.gatherConfigs();
        conf.addResource("hdfs-site.prairie.xml");
        conf.set("hdfs.minidfs.basedir", getTmpDir().toString());
        conf.set("hadoop.proxyuser." + user + ".hosts", "*");
        conf.set("hadoop.proxyuser." + user + ".groups", "*");
        return conf;
    }

    private void createHomeDirectory() throws IOException {
        getFileSystem().mkdirs(new org.apache.hadoop.fs.Path("/user/", user));
        getFileSystem().setOwner(new org.apache.hadoop.fs.Path("/user/", user), user, user);
    }

    @Override
    public void destroy() throws DestroyUnitException {
        try {
            getFileSystem().close();
        } catch (IOException e) {
            throw new DestroyUnitException(e);
        }
        miniDFSCluster.shutdown();
    }

    public String getNamenode() {
        return "hdfs://" + miniDFSCluster.getNameNode().getNameNodeAddress().getHostName() + ":"
                + miniDFSCluster.getNameNode().getNameNodeAddress().getPort();
    }

    public FileSystem getFileSystem() throws IOException {
        return miniDFSCluster.getFileSystem();
    }

    public Configuration getConfig() {
        return miniDFSCluster.getConfiguration(0);
    }

    public <T> void saveAs(InputStream inputStream, String dstPath, Format<T> inputFormat, Format<T> outputFormat) throws IOException {
        Path outPath = new Path(dstPath);
        getFileSystem().mkdirs(outPath.getParent());
        FSDataOutputStream fsDataOutputStream = getFileSystem().create(outPath);
        try {
            InputFormatReader<T> inputFormatReader = inputFormat.createReader(inputStream);
            OutputFormatWriter<T> outputFormatWriter = outputFormat.createWriter(fsDataOutputStream);
            T line;
            while ((line = inputFormatReader.next()) != null) {
                outputFormatWriter.write(line);
            }
            inputFormatReader.close();
            outputFormatWriter.close();
        } catch (FormatException e) {
            throw new IOException(e);
        }
    }

    public <T> CompareResponse<T> compare(Path path, Format<T> format,
                                          InputStream expectedStream, Format<T> expectedFormat) throws IOException {
        InputStream inputStream = null;
        if (getFileSystem().isDirectory(path)) {
            for (FileStatus fileStatus : getFileSystem().listStatus(path)) {
                if (fileStatus.isDirectory()) {
                    throw new IOException(fileStatus.getPath().toString() + " is directory");
                }
                InputStream fileStream = getFileSystem().open(fileStatus.getPath());
                if (inputStream == null) {
                    inputStream = fileStream;
                } else {
                    inputStream = new SequenceInputStream(inputStream, fileStream);
                }
            }
        } else {
            inputStream = getFileSystem().open(path);
        }
        try {
            InputFormatReader<T> reader = format.createReader(inputStream);
            InputFormatReader<T> expectedReader = expectedFormat.createReader(expectedStream);
            CompareResponse<T> compareResponse = byLineComparator.compare(expectedReader.all(), reader.all());
            expectedReader.close();
            expectedReader.close();
            return compareResponse;
        } catch (FormatException e) {
            throw new IOException(e);
        }
    }

    public <T> CompareResponse<T> compare(Path path, Format<T> format,
                                          Path expectedPath, Format<T> expectedFormat) throws IOException {
        InputStream retrieved = getFileSystem().open(expectedPath);
        return compare(path, format, retrieved, expectedFormat);
    }

    public <T> CompareResponse<T> compare(Path path, Format<T> format,
                                          String expectedResource, Format<T> expectedFormat) throws IOException {
        InputStream expectedStream = HdfsUnit.class.getClassLoader().getResourceAsStream(expectedResource);
        return compare(path, format, expectedStream, expectedFormat);
    }

    private static Version getVersion() {
        Properties properties = new Properties();
        try {
            properties.load(HdfsUnit.class.getClassLoader().getResourceAsStream("META-INF/maven/org.apache.hadoop/hadoop-hdfs/pom.properties"));
        } catch (IOException e) {
            LOGGER.error("Can't load hdfs version", e);
        }
        return new Version(properties.getProperty("version", "unknown"));
    }

}
