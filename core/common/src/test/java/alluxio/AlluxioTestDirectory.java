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

package alluxio;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Class for managing creation and cleanup of an Alluxio test directory.
 *
 * The Alluxio test directory is created as a subdirectory of the system temp directory, and on
 * class initialization it deletes files and directories older than the maximum age.
 */
public final class AlluxioTestDirectory {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioTestDirectory.class);

  private static final int MAX_FILE_AGE_HOURS = 1;

  /**
   * This directory should be used over the system temp directory for creating temporary files
   * during tests. We recursively delete this directory on JVM exit.
   */
  public static final File ALLUXIO_TEST_DIRECTORY = createTestingDirectory();

  /**
   * Creates a directory with the given prefix inside the Alluxio temporary directory.
   *
   * @param prefix a prefix to use in naming the temporary directory
   * @return the created directory
   */
  public static File createTemporaryDirectory(String prefix) {
    return createTemporaryDirectory(ALLUXIO_TEST_DIRECTORY, prefix);
  }

  /**
   * Creates a directory with the given prefix inside the Alluxio temporary directory.
   *
   * @param parent a parent directory
   * @param prefix a prefix to use in naming the temporary directory
   * @return the created directory
   */
  public static File createTemporaryDirectory(File parent, String prefix) {
    final File file = new File(parent, prefix + "-" + UUID.randomUUID());
    if (!file.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + file.getAbsolutePath());
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> delete(file)));
    return file;
  }

  /**
   * Creates the Alluxio testing directory and attempts to delete old files from the previous one.
   *
   * @return the testing directory
   */
  private static File createTestingDirectory() {
    final File tmpDir = new File(System.getProperty("java.io.tmpdir"), "alluxio-tests");
    if (tmpDir.exists()) {
      cleanUpOldFiles(tmpDir);
    }
    if (!tmpDir.exists()) {
      if (!tmpDir.mkdir()) {
        throw new RuntimeException(
            "Failed to create testing directory " + tmpDir.getAbsolutePath());
      }
    }
    return tmpDir;
  }

  /**
   * Deletes files in the given directory which are older than 2 hours.
   *
   * @param dir the directory to clean up
   */
  private static void cleanUpOldFiles(File dir) {
    long cutoffTimestamp = System.currentTimeMillis() - (MAX_FILE_AGE_HOURS * Constants.HOUR_MS);
    Arrays.asList(dir.listFiles()).stream()
        .filter(file -> !FileUtils.isFileNewer(file, cutoffTimestamp))
        .forEach(AlluxioTestDirectory::delete);
  }

  private static void delete(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isFile()) {
      file.delete();
      return;
    }
    try {
      FileUtils.deleteDirectory(file);
    } catch (IOException e) {
      LOG.warn("Failed to clean up {} : {}", file.getAbsolutePath(), e.toString());
    }
  }
}
