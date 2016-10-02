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
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.DeleteOptions;
import alluxio.examples.BasicNonByteBufferOperations;
import alluxio.examples.BasicOperations;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Driver to run Alluxio tests.
 */
@ThreadSafe
public final class TestRunner {
  /**
   * The operation types to test.
   */
  enum OperationType {
    /**
     * Basic operations.
     */
    Basic,
    /**
     * Basic operations but not using ByteBuffer.
     */
    BasicNonByteBuffer,
  }

  /** Read types to test. */
  private static final List<ReadType> READ_TYPES =
      Arrays.asList(ReadType.CACHE_PROMOTE, ReadType.CACHE, ReadType.NO_CACHE);

  /** Write types to test. */
  private static final List<WriteType> WRITE_TYPES = Arrays
      .asList(WriteType.MUST_CACHE, WriteType.CACHE_THROUGH, WriteType.THROUGH,
          WriteType.ASYNC_THROUGH);

  private TestRunner() {} // prevent instantiation

  private static void usage() {
    System.out.println("Usage:");
    System.out.println("(1) To run a predefined set of read/write tests:");
    System.out.println(String.format("java -cp %s %s <master address>",
        RuntimeConstants.ALLUXIO_JAR, TestRunner.class.getCanonicalName()));
    System.out.println("(2) To run a single test:");
    System.out.println(String
        .format("java -cp %s %s <master address> <%s> <%s> <%s>", RuntimeConstants.ALLUXIO_JAR,
            TestRunner.class.getCanonicalName(), OperationType.class.getSimpleName(),
            ReadType.class.getSimpleName(), WriteType.class.getSimpleName()));
    System.out.println(String.format("\t <%s>: one of %s", OperationType.class.getSimpleName(),
        Arrays.toString(OperationType.values())));
    System.out
        .println(String.format("\t <%s>: one of %s", ReadType.class.getSimpleName(), READ_TYPES));
    System.out
        .println(String.format("\t <%s>: one of %s", WriteType.class.getSimpleName(), WRITE_TYPES));
  }

  /** Directory for the test generated files. */
  public static final String TEST_PATH = "/default_tests_files";

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   * @throws Exception if error occurs during tests
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1 && args.length != 4) {
      usage();
      System.exit(-1);
    }
    AlluxioURI masterLocation = new AlluxioURI(args[0]);
    AlluxioURI testDir = new AlluxioURI(TEST_PATH);
    Configuration.set(PropertyKey.MASTER_HOSTNAME, masterLocation.getHost());
    Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(masterLocation.getPort()));
    ClientContext.init();

    FileSystem fs = FileSystem.Factory.get();
    if (fs.exists(testDir)) {
      fs.delete(testDir, DeleteOptions.defaults().setRecursive(true));
    }
    int ret;
    if (args.length == 1) {
      ret = runTests(masterLocation);
    } else {
      OperationType optType = OperationType.valueOf(args[1]);
      ReadType readType = ReadType.valueOf(args[2]);
      WriteType writeType = WriteType.valueOf(args[3]);
      ret = runTest(masterLocation, optType, readType, writeType);
    }

    System.exit(ret);
  }

  /**
   * Runs all combinations of tests of operation, read and write type.
   *
   * @param masterLocation master address
   * @return 0 on success, 1 on failure
   */
  private static int runTests(AlluxioURI masterLocation) {
    int failed = 0;
    for (ReadType readType : READ_TYPES) {
      for (WriteType writeType : WRITE_TYPES) {
        for (OperationType opType : OperationType.values()) {
          System.out.println(String.format("runTest %s %s %s", opType, readType, writeType));
          failed += runTest(masterLocation, opType, readType, writeType);
        }
      }
    }
    if (failed > 0) {
      System.out.println("Number of failed tests: " + failed);
    }
    return failed;
  }

  /**
   * Runs a single test give operation, read and write type.
   *
   * @param masterLocation master address
   * @param opType operation type
   * @param readType read type
   * @param writeType write type
   * @return 0 on success, 1 on failure
   */
  private static int runTest(AlluxioURI masterLocation, OperationType opType, ReadType readType,
      WriteType writeType) {
    AlluxioURI filePath =
        new AlluxioURI(String.format("%s/%s_%s_%s", TEST_PATH, opType, readType, writeType));

    boolean result = true;
    switch (opType) {
      case Basic:
        result =
            CliUtils.runExample(new BasicOperations(filePath, readType, writeType));
        break;
      case BasicNonByteBuffer:
        result = CliUtils.runExample(
            new BasicNonByteBufferOperations(filePath, readType, writeType, true,
                20));
        break;
      default:
        System.out.println("Unrecognized operation type " + opType);
    }
    return result ? 0 : 1;
  }
}
