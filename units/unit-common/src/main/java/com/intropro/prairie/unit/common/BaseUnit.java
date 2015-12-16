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
package com.intropro.prairie.unit.common;

import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 9/18/15.
 */
public abstract class BaseUnit implements Unit {

    private static final Logger LOGGER = LogManager.getLogger(BaseUnit.class);

    private static final Path GLOBAL_TMP_DIR;

    static {
        try {
            Path globalTmpPath = Paths.get(System.getProperty("java.io.tmpdir"), "prairie");
            LOGGER.info("Creating global tmp directory: " + globalTmpPath);
            GLOBAL_TMP_DIR = Files.createDirectories(globalTmpPath);
            GLOBAL_TMP_DIR.toFile().setExecutable(true, false);
            GLOBAL_TMP_DIR.toFile().setReadable(true, false);
            GLOBAL_TMP_DIR.toFile().setWritable(true, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String unitName;

    private Path tmpDir;

    public BaseUnit(String unitName) {
        this.unitName = unitName;
        clearOldDTmpDirectories();
        try {
            tmpDir = Files.createTempDirectory(GLOBAL_TMP_DIR, unitName);
            tmpDir.toFile().setExecutable(true, false);
            tmpDir.toFile().setReadable(true, false);
            tmpDir.toFile().setWritable(true, false);
            LOGGER.info("Created " + unitName + " tmp directory: " + tmpDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create tmp dir inside: " + GLOBAL_TMP_DIR, e);
        }
    }

    protected Path getTmpDir() {
        return tmpDir;
    }

    @Override
    public void start() throws InitUnitException {
        LOGGER.info(String.format("Starting %s unit", unitName));
        init();
        LOGGER.info(String.format("Unit %s started", unitName));
    }

    @Override
    public final void stop() throws DestroyUnitException {
        LOGGER.info(String.format("Destroying %s unit", unitName));
        destroy();
        try {
            clearTmpDirectory(tmpDir);
        } catch (IOException e) {
            throw new DestroyUnitException("Failed to delete tmp dir: " + tmpDir, e);
        }
        LOGGER.info(String.format("Unit %s destroyed", unitName));
    }

    private void clearTmpDirectory(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return super.visitFile(file, attrs);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return super.postVisitDirectory(dir, exc);
            }
        });
    }

    private void clearOldDTmpDirectories() {
        File[] oldDirs = GLOBAL_TMP_DIR.toFile().listFiles();
        if (oldDirs != null) {
            for (File oldDir : oldDirs) {
                if (oldDir.lastModified() < System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1)
                        && !oldDir.isHidden() && oldDir.isDirectory()) {
                    try {
                        clearTmpDirectory(oldDir.toPath());
                    } catch (IOException e) {
                        LOGGER.warn("Can't delete old directory: " + oldDir, e);
                    }
                }
            }
        }
    }

    public String getUnitName() {
        return unitName;
    }

    protected abstract void destroy() throws DestroyUnitException;

    protected abstract void init() throws InitUnitException;

    protected String replacePlaceholders(String script, Map<String, String> configurations) {
        for (Map.Entry<String, String> configuration : configurations.entrySet()) {
            script = script.replaceAll("\\$\\{" + configuration.getKey() + "\\}", configuration.getValue());
        }
        return script;
    }

}
