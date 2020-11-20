/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.bundler;

import static org.junit.Assert.assertEquals;

import alluxio.util.CommonUtils;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InfoCollectorTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(InfoCollectorTestUtils.class);

  public static File createTemporaryDirectory() {
    final File file = Files.createTempDir();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        FileUtils.deleteDirectory(file);
      } catch (IOException e) {
        LOG.warn("Failed to clean up {} : {}", file.getAbsolutePath(), e.toString());
      }
    }));
    return file;
  }

  public static File createFileInDir(File dir, String fileName) throws IOException {
    File newFile = new File(Paths.get(dir.getCanonicalPath(), fileName).toUri());
    newFile.createNewFile();
    return newFile;
  }

  public static File createDirInDir(File dir, String dirName) throws IOException {
    File newDir = new File(Paths.get(dir.getCanonicalPath(), dirName).toUri());
    newDir.mkdir();
    return newDir;
  }

  public static void verifyAllFiles(File targetDir, Set<String> expectedFiles) throws IOException {
    Set<String> copiedFiles = getAllFileNamesRelative(targetDir, targetDir);
    assertEquals(expectedFiles, copiedFiles);
  }

  public static Set<String> getAllFileNamesRelative(File dir, File baseDir) throws IOException {
    if (!dir.isDirectory()) {
      throw new IOException(String.format("Expected a directory but found a file at %s%n",
              dir.getCanonicalPath()));
    }
    Set<String> fileSet = new HashSet<>();
    List<File> allFiles = CommonUtils.recursiveListLocalDir(dir);
    for (File f : allFiles) {
      String relativePath = baseDir.toURI().relativize(f.toURI()).toString();
      fileSet.add(relativePath);
    }
    return fileSet;
  }
}
