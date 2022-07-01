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

package alluxio.fuse.correctness;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Random;

/**
 * This class contains utility functions for validating fuse correctness.
 */
public class Utils {
  public static final Random RANDOM = new Random();
  private static final ThreadLocalRandom THREAD_LOCAL_RANDOM = ThreadLocalRandom.current();

  /**
   * Creates a file given size and directory.
   *
   * @param size file size
   * @param dirPath the dir where the created file is
   * @return the path of the created file
   */
  public static String createFile(long size, String dirPath) {
    File dir = new File(dirPath);
    if (!dir.exists() && !dir.mkdirs()) {
      throw new RuntimeException(
          "Failed to create local directory specified. Test is stopped.");
    }
    String localFilePath = Paths.get(
        dirPath, Long.toString(System.currentTimeMillis())).toString();
    try (FileOutputStream outputStream = new FileOutputStream(localFilePath)) {
      int bufferSize = (int) Math.min(Constants.DEFAULT_BUFFER_SIZE, size);
      byte[] buf = new byte[bufferSize];
      while (size > 0) {
        RANDOM.nextBytes(buf);
        int sizeToWrite = (int) Math.min(bufferSize, size);
        outputStream.write(buf, 0, sizeToWrite);
        size -= sizeToWrite;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test file. Test is stopped.", e);
    }
    return localFilePath;
  }

  /**
   * Copies the local testing file into the mount point of AlluxioFuse.
   *
   * @param srcFilePath the path of the local testing file
   * @param mountPoint the mount point of AlluxioFuse
   * @return the path of the copied file
   */
  public static String copyLocalFileToFuseMountPoint(String srcFilePath, String mountPoint) {
    Path fuseFilePath = Paths.get(mountPoint, Long.toString(System.currentTimeMillis()));
    try {
      Files.copy(Paths.get(srcFilePath), fuseFilePath);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to copy local test file into Alluxio. Test is stopped.", e);
    }
    return fuseFilePath.toString();
  }

  /**
   * Checks if the two buffers contain the same data.
   *
   * @param localFileBuffer the buffer contains the local file content
   * @param fuseFileBuffer the buffer contains the fuse file content
   * @param localBytesRead the valid number of bytes in the localFileBuffer
   * @param fuseBytesRead the valid number of bytes in the fuseBytesBuffer
   * @return true if they contain the same data; false otherwise
   */
  public static boolean isDataCorrect(
      byte[] localFileBuffer, byte[] fuseFileBuffer, int localBytesRead, int fuseBytesRead) {
    if (localBytesRead != fuseBytesRead) {
      return false;
    }
    for (int i = 0; i < localBytesRead; i++) {
      if (localFileBuffer[i] != fuseFileBuffer[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Deletes target files.
   *
   * @param localFilePath the path of the local file
   * @param fuseFilePath the path of the fuse file
   */
  public static void deleteTestFiles(String localFilePath, String fuseFilePath) {
    try {
      Files.delete(Paths.get(localFilePath));
      Files.delete(Paths.get(fuseFilePath));
    } catch (IOException e) {
      System.out.println("Error deleting testing files. " + e);
    }
  }

  /**
   * Generates a long within the bound.
   *
   * @param bound the upperbound of the random long
   * @return a random long number
   */
  public static long nextRandomLong(long bound) {
    return THREAD_LOCAL_RANDOM.nextLong(bound);
  }

  private Utils() {}
}
