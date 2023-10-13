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
import alluxio.Constants;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WorkerNetAddress;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;
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

  @Parameter(names = {"--workers", "--worker"},
      description = "Alluxio worker addresses to run tests on. "
          + "If not specified, random ones will be used.",
      converter = WorkerAddressConverter.class)
  @Nullable
  @SuppressFBWarnings(value = "UWF_NULL_FIELD", justification = "Injected field")
  private List<WorkerNetAddress> mWorkerAddresses = null;

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
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) throws Exception {
    TestRunner runner = new TestRunner();
    JCommander jCommander = new JCommander(runner);
    jCommander.setProgramName("TestRunner");
    jCommander.parse(args);
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
    if (mWorkerAddresses != null) {
      Configuration.set(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED, false);
    }

    mDirectory = PathUtils.concatPath(mDirectory, TEST_DIRECTORY_NAME);

    AlluxioURI testDir = new AlluxioURI(mDirectory);
    FileSystemContext fsContext = FileSystemContext.create();
    FileSystem fs = FileSystem.Factory.create(fsContext);
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

    if (mWorkerAddresses == null) {
      for (ReadType readType : readTypes) {
        for (WriteType writeType : writeTypes) {
          for (OperationType opType : operations) {
            System.out.println(String.format("runTest --operation %s --readType %s --writeType %s",
                opType, readType, writeType));
            failed += runTest(opType, readType, writeType, fsContext, null);
          }
        }
      }
      if (failed > 0) {
        System.out.println("Number of failed tests: " + failed);
      }
    } else {
      // If workers are specified, the test will iterate through all workers and
      // for each worker:
      // 1. Create a file
      // 2. Write the blocks of test files into a specific worker using SpecificHostPolicy
      // 3. Open the file to read
      // 4. If blocks are cached on the worker (MUST_CACHE/CACHE_THROUGH/ASYNC_THROUGH),
      //    then data will be read from that specific worker as there is only one copy of data.
      //    If blocks are not cached on the worker (THROUGH),
      //    then data will be loaded from UFS into that specific worker, by setting the
      //    ufsReadWorkerLocation field InStreamOptions.
      // In this way, we made sure that a worker works normally on both reads and writes.
      HashMap<WorkerNetAddress, Integer> failedTestWorkers = new HashMap<>();
      boolean hasFailedWorkers = false;
      for (WorkerNetAddress workerNetAddress : mWorkerAddresses) {
        System.out.println("Running test for worker:" + getWorkerAddressString(workerNetAddress));
        for (ReadType readType : readTypes) {
          for (WriteType writeType : writeTypes) {
            for (OperationType opType : operations) {
              System.out.printf("[%s] runTest --operation %s --readType %s --writeType %s%n",
                  getWorkerAddressString(workerNetAddress), opType, readType, writeType);
              failed += runTest(opType, readType, writeType, fsContext, workerNetAddress);
              failedTestWorkers.put(
                  workerNetAddress, failedTestWorkers.getOrDefault(workerNetAddress, 0) + failed);
              if (failed != 0) {
                hasFailedWorkers = true;
              }
            }
          }
        }
      }
      if (!hasFailedWorkers) {
        System.out.println(
            Constants.ANSI_GREEN + "All workers passed tests!" + Constants.ANSI_RESET);
      } else {
        System.out.println(
            Constants.ANSI_RED + "Some workers failed tests!" + Constants.ANSI_RESET);
        failedTestWorkers.forEach((k, v) -> {
          if (v > 0) {
            System.out.printf(
                "%sWorker %s failed %s tests %s%n",
                Constants.ANSI_RED, getWorkerAddressString(k), 4, Constants.ANSI_RESET);
          }
        });
      }
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
  private int runTest(
      OperationType opType, ReadType readType, WriteType writeType,
      FileSystemContext fsContext, @Nullable WorkerNetAddress workerAddress) {
    final AlluxioURI filePath;
    if (workerAddress == null) {
      filePath = new AlluxioURI(
          String.format("%s/%s_%s_%s", mDirectory, opType, readType, writeType));
    } else {
      String workerAddressString = getWorkerAddressString(workerAddress);
      filePath = new AlluxioURI(
          String.format("%s/%s/%s_%s_%s", mDirectory, workerAddressString, opType, readType,
              writeType));
    }

    boolean result = true;
    switch (opType) {
      case BASIC:
        result = RunTestUtils.runExample(
          new BasicOperations(filePath, readType, writeType, fsContext, workerAddress));
        break;
      case BASIC_NON_BYTE_BUFFER:
        result = RunTestUtils.runExample(
          new BasicNonByteBufferOperations(
              filePath, readType, writeType, true, 20, fsContext, workerAddress));
        break;
      default:
        System.out.println("Unrecognized operation type " + opType);
    }
    return result ? 0 : 1;
  }

  /**
   * Parses worker address param.
   */
  public static class WorkerAddressConverter implements IStringConverter<WorkerNetAddress> {
    @Override
    public WorkerNetAddress convert(String s) {
      if (s.contains(":")) {
        String[] components = s.split(":");
        Preconditions.checkState(components.length == 2);
        return WorkerNetAddress.newBuilder().setHost(components[0])
            .setRpcPort(Integer.parseInt(components[1])).build();
      } else {
        return WorkerNetAddress.newBuilder().setHost(s).build();
      }
    }
  }

  private String getWorkerAddressString(WorkerNetAddress workerAddress) {
    return workerAddress.getRpcPort() == 0 ? workerAddress.getHost() :
        workerAddress.getHost() + "_" + workerAddress.getRpcPort();
  }
}
