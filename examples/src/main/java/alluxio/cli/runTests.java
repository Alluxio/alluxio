/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.DeleteOptions;
import alluxio.examples.BasicNonByteBufferOperations;
import alluxio.examples.BasicOperations;
import alluxio.examples.Utils;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Driver to run Alluxio tests.
 */
@NotThreadSafe
public final class RunTests {
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

  private static void usage() {
    System.out.println("Usage:");
    System.out.println(String
        .format("runTest: java -cp %s %s <master address>", Version.ALLUXIO_JAR,
            RunTests.class.getName()));
    System.out.println(String
        .format("runTest: java -cp %s %s <master address> <%s> <%s> <%s>", Version.ALLUXIO_JAR,
            RunTests.class.getName(), OperationType.class.getSimpleName(),
            ReadType.class.getSimpleName(), WriteType.class.getSimpleName()));
    System.out.println(String
        .format("\t Possible values for %s: %s", OperationType.class.getSimpleName(),
            OperationType.values()));
    System.out.println(String
        .format("\t Possible values for %s: %s", ReadType.class.getSimpleName(), READ_TYPES));
    System.out.println(String
        .format("\t Possible values for %s: %s", WriteType.class.getSimpleName(), WRITE_TYPES));
  }

  public static final String TEST_PATH = "/default_tests_files";

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1 && args.length != 4) {
      usage();
      System.exit(-1);
    }
    AlluxioURI masterLocation = new AlluxioURI(args[0]);
    AlluxioURI testDir = new AlluxioURI(TEST_PATH);
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
    } if (failed > 0) {
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
    if (opType == OperationType.Basic) {
      result = Utils.runExample(new BasicOperations(masterLocation, filePath, readType, writeType));
    } else if (opType == OperationType.BasicNonByteBuffer) {
      result = Utils.runExample(
          new BasicNonByteBufferOperations(masterLocation, filePath, readType, writeType, true,
              20));
    }
    return result ? 0 : 1;
  }
}
