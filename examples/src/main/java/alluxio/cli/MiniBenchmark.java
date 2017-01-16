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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mini benchmark that writes/reads one file with a given size and operation type.
 */
@ThreadSafe
public final class MiniBenchmark {
  /**
   * The operation types to test.
   */
  enum OperationType {
    /**
     * Read from a file.
     */
    READ,
    /**
     * Write to a file.
     */
    WRITE,
  }

  private MiniBenchmark() {} // prevent instantiation

  private static void usage() {
    System.out.println("Usage:");
    System.out.println("To run a mini benchmark to read or write a file.");
    System.out.println(String
        .format("java -cp %s %s <master address> <[READ, WRITE]> <fileSize> <iterations>",
        RuntimeConstants.ALLUXIO_JAR, MiniBenchmark.class.getCanonicalName()));
  }

  /** Directory for the test generated files. */
  public static final String TEST_PATH = "/default_mini_benchmark";

  /**
   * @param args there are no arguments needed
   * @throws Exception if error occurs during tests
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      usage();
      System.exit(-1);
    }
    AlluxioURI masterLocation = new AlluxioURI(args[0]);
    Configuration.set(PropertyKey.MASTER_HOSTNAME, masterLocation.getHost());
    Configuration.set(PropertyKey.MASTER_RPC_PORT, masterLocation.getPort());
    FileSystemContext.INSTANCE.reset();

    OperationType operationType = Enum.valueOf(OperationType.class, args[1]);
    long fileSize = FormatUtils.parseSpaceSize(args[2]);
    int iterations = Integer.parseInt(args[3]);

    CommonUtils.warmUpLoop();

    switch (operationType) {
      case READ:
        readFile(fileSize, iterations);
        break;
      case WRITE:
        writeFile(fileSize, iterations);
        break;
      default:
        throw new RuntimeException("Unsupported type.");
    }
  }

  /**
   * Reads a file.
   *
   * @param fileSize the file size
   * @param iterations the number of iterations to run
   * @throws Exception if it fails to read
   */
  private static void readFile(long fileSize, int iterations) throws Exception {
    FileSystem fileSystem = FileSystem.Factory.get();
    byte[] buffer = new byte[(int) Math.min(fileSize, 4 * Constants.MB)];

    long start = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      try (FileInStream inStream = fileSystem.openFile(new AlluxioURI(TEST_PATH))) {
        while (inStream.read(buffer) != -1) {}
      }
    }
    System.out.printf("Runtime: %f seconds.%n",
        (System.nanoTime() - start) * 1.0 / Constants.SECOND_NANO);
  }

  /**
   * Writes a file.
   *
   * @param fileSize the file size
   * @param iterations number of iterations
   * @throws Exception it if fails to write
   */
  private static void writeFile(long fileSize, int iterations) throws Exception {
    FileSystem fileSystem = FileSystem.Factory.get();
    byte[] buffer = new byte[(int) Math.min(fileSize, 4 * Constants.MB)];
    Arrays.fill(buffer, (byte) 'a');
    AlluxioURI path = new AlluxioURI(TEST_PATH);

    long runTime = 0;
    for (int i = 0; i < iterations; i++) {
      if (fileSystem.exists(path)) {
        fileSystem.delete(path);
      }

      long bytesWritten = 0;
      long start = System.nanoTime();
      try (FileOutStream outStream = fileSystem.createFile(new AlluxioURI(TEST_PATH))) {
        while (bytesWritten < fileSize) {
          outStream.write(buffer, 0, (int) Math.min(buffer.length, fileSize - bytesWritten));
          bytesWritten += buffer.length;
        }
      }
      runTime += System.nanoTime() - start;
    }

    System.out.printf("Runtime: %f seconds.%n", runTime * 1.0 / Constants.SECOND_NANO);
  }
}
