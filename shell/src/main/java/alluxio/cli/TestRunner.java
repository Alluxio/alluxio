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
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.DeletePOptions;
import alluxio.util.ConfigurationUtils;
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
  private static final List<ReadType> READ_TYPES =
      Arrays.asList(ReadType.CACHE_PROMOTE, ReadType.CACHE, ReadType.NO_CACHE);

  /** Write types to test. */
  private static final List<WriteType> WRITE_TYPES = Arrays.asList(WriteType.MUST_CACHE,
      WriteType.CACHE_THROUGH, WriteType.THROUGH, WriteType.ASYNC_THROUGH);

  @Parameter(names = "--directory",
      description = "Alluxio path for the tests working directory.")
  private String mDirectory = AlluxioURI.SEPARATOR;

  @Parameter(names = {"-h", "--help"}, description = "Prints usage information", help = true)
  private boolean mHelp;

  @Parameter(names = "--operation", description = "The operation to test, either BASIC or "
      + "BASIC_NON_BYTE_BUFFER. By default both operations are tested.")
  private String mOperation;

  @Parameter(names = "--readType",
      description = "The read type to use, one of NO_CACHE, CACHE, CACHE_PROMOTE. "
      + "By default all readTypes are tested.")
  private String mReadType;

  @Parameter(names = "--writeType",
      description = "The write type to use, one of MUST_CACHE, CACHE_THROUGH, "
      + "THROUGH, ASYNC_THROUGH. By default all writeTypes are tested.")
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
    FileSystemContext fsContext =
        FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
    FileSystem fs =
        FileSystem.Factory.create(fsContext);
    if (fs.exists(testDir)) {
      fs.delete(testDir, DeletePOptions.newBuilder().setRecursive(true).setUnchecked(true).build());
    }

    int failed = 0;

    List<ReadType> readTypes =
        mReadType == null ? READ_TYPES : Lists.newArrayList(ReadType.valueOf(mReadType));
    List<WriteType> writeTypes =
        mWriteType == null ? WRITE_TYPES : Lists.newArrayList(WriteType.valueOf(mWriteType));
    List<OperationType> operations = mOperation == null ? Lists.newArrayList(OperationType.values())
        : Lists.newArrayList(OperationType.valueOf(mOperation));

    for (ReadType readType : readTypes) {
      for (WriteType writeType : writeTypes) {
        for (OperationType opType : operations) {
          System.out.println(String.format("runTest --operation %s --readType %s --writeType %s",
                  opType, readType, writeType));
          failed += runTest(opType, readType, writeType, fsContext);
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
  private int runTest(OperationType opType, ReadType readType, WriteType writeType,
      FileSystemContext fsContext) {
    AlluxioURI filePath =
        new AlluxioURI(String.format("%s/%s_%s_%s", mDirectory, opType, readType, writeType));

    boolean result = true;
    switch (opType) {
      case BASIC:
        result = RunTestUtils.runExample(
          new BasicOperations(filePath, readType, writeType, fsContext));
        break;
      case BASIC_NON_BYTE_BUFFER:
        result = RunTestUtils.runExample(
          new BasicNonByteBufferOperations(filePath, readType, writeType, true, 20, fsContext));
        break;
      default:
        System.out.println("Unrecognized operation type " + opType);
    }
    return result ? 0 : 1;
  }
}
