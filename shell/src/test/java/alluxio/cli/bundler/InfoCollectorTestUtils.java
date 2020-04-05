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

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

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
    File newFile = new File(Paths.get(dir.getAbsolutePath(), fileName).toString());
    newFile.createNewFile();
    return newFile;
  }

  public static void create() {
    Files.createTempDir();
  }
}
