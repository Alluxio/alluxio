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
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.examples.BasicNonByteBufferOperations;
import alluxio.examples.BasicOperations;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Driver to run Alluxio tests.
 */
@ThreadSafe
public final class TestRunner {
  /** Read types to test. */
  private static final List<ReadPType> READ_TYPES =
      Arrays.asList(ReadPType.READ_CACHE_PROMOTE, ReadPType.READ_CACHE, ReadPType.READ_NO_CACHE);

  /** Write types to test. */
  private static final List<WritePType> WRITE_TYPES = Arrays.asList(WritePType.WRITE_MUST_CACHE,
      WritePType.WRITE_CACHE_THROUGH, WritePType.WRITE_THROUGH, WritePType.WRITE_ASYNC_THROUGH);

  @Parameter(names = "--directory",
      description = "Alluxio path for the tests working directory.")
  private String mDirectory = AlluxioURI.SEPARATOR;

  @Parameter(names = {"-h", "--help"}, description = "Prints usage information", help = true)
  private boolean mHelp;

  @Parameter(names = "--operation", description = "The operation to test, either BASIC or "
      + "BASIC_NON_BYTE_BUFFER. By default both operations are tested.")
  private String mOperation;

  @Parameter(names = "--readType",
      description = "The read type to use. By default all readTypes are tested.")
  private String mReadType;

  @Parameter(names = "--writeType",
      description = "The write type to use. By default all writeTypes are tested.")
  private String mWriteType;

  /**
   * The operation types to test.
   */
  enum OperationType {
    /**
     * Basic operations.
     */
    BASIC,
    /**
     * Basic operations but not using ByteBuffer.
     */
    BASIC_NON_BYTE_BUFFER,
  }

  private TestRunner() {} // prevent instantiation

  /** Directory for the test generated files. */
  public static final String TEST_DIRECTORY_NAME = "default_tests_files";

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   */
  public static void main(String[] args) throws Exception {
    TestRunner runner = new TestRunner();
    JCommander jCommander = new JCommander(runner, args);
    jCommander.setProgramName("TestRunner");
    if (runner.mHelp) {
      jCommander.usage();
      return;
    }

    int ret = runner.runTests();
    System.exit(ret);
  }

  /**
   * Runs combinations of tests of operation, read and write type.
   *
   * @return the number of failed tests
   */
  private int runTests() throws Exception {
    mDirectory = PathUtils.concatPath(mDirectory, TEST_DIRECTORY_NAME);

    AlluxioURI testDir = new AlluxioURI(mDirectory);
    FileSystem fs = FileSystem.Factory.get();
    if (fs.exists(testDir)) {
      fs.delete(testDir, FileSystemClientOptions.getDeleteOptions().toBuilder().setRecursive(true)
          .setUnchecked(true).build());
    }

    int failed = 0;

    // TODO(ggezer) ReadPType conversion
    List<ReadPType> readTypes =
        mReadType == null ? READ_TYPES : Lists.newArrayList(ReadPType.valueOf("READ_" + mReadType));
    // TODO(ggezer) WritePType conversion
    List<WritePType> writeTypes = mWriteType == null ? WRITE_TYPES
        : Lists.newArrayList(WritePType.valueOf("WRITE_" + mWriteType));
    List<OperationType> operations = mOperation == null ? Lists.newArrayList(OperationType.values())
        : Lists.newArrayList(OperationType.valueOf(mOperation));

    for (ReadPType readType : readTypes) {
      for (WritePType writeType : writeTypes) {
        for (OperationType opType : operations) {
          System.out.println(String.format("runTest %s %s %s", opType, readType, writeType));
          failed += runTest(opType, readType, writeType);
        }
      }
    }
    if (failed > 0) {
      System.out.println("Number of failed tests: " + failed);
    }
    return failed;
  }

  /**
   * Runs a single test given operation, read and write type.
   *
   * @param opType operation type
   * @param readType read type
   * @param writeType write type
   * @return 0 on success, 1 on failure
   */
  private int runTest(OperationType opType, ReadPType readType, WritePType writeType) {
    AlluxioURI filePath =
        new AlluxioURI(String.format("%s/%s_%s_%s", mDirectory, opType, readType, writeType));

    boolean result = true;
    switch (opType) {
      case BASIC:
        result = CliUtils.runExample(new BasicOperations(filePath, readType, writeType));
        break;
      case BASIC_NON_BYTE_BUFFER:
        result = CliUtils
            .runExample(new BasicNonByteBufferOperations(filePath, readType, writeType, true, 20));
        break;
      default:
        System.out.println("Unrecognized operation type " + opType);
    }
    return result ? 0 : 1;
  }
}
