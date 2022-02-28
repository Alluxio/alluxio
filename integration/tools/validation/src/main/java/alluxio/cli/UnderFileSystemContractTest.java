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

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Integration tests for Alluxio under filesystems. It describes the contract of Alluxio
 * with the UFS through the UFS interface.
 *
 * This class will run all tests (with "Test" suffix in the method name)
 * in {@link UnderFileSystemCommonOperations}. If the given ufs path is a S3 path,
 * all tests in {@link S3ASpecificOperations} will also be run.
 */
public final class UnderFileSystemContractTest {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemContractTest.class);

  public static final String TASK_NAME = "ValidateUfsOperations";
  private static final String S3_IDENTIFIER = "s3";

  @Parameter(names = {"--path"}, required = true,
      description = "The under filesystem path to run tests against.")
  private String mUfsPath;

  @Parameter(names = {"--test"}, required = false,
      description = "Test name, this option can be passed multiple times to "
          + "indicate multiply tests")
  private List<String> mTestList = new ArrayList<>();

  @Parameter(names = {"--help"}, help = true)
  private boolean mHelp = false;

  private InstancedConfiguration mConf;
  private UnderFileSystem mUfs;

  /**
   * A constructor from default.
   * */
  public UnderFileSystemContractTest() {
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  /**
   * Initiate the tests for a specific UFS path and UFS configs.
   *
   * @param path the UFS path
   * @param conf the UFs configurations
   * */
  public UnderFileSystemContractTest(String path, InstancedConfiguration conf) {
    mUfsPath = path;
    mConf = conf;
  }

  /**
   * Runs the tests and returns nothing.
   * */
  public void run() throws Exception {
    UnderFileSystemConfiguration ufsConf = getUfsConf();
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find(mUfsPath, ufsConf);
    // Check if the ufs path is valid
    if (factory == null || !factory.supportsPath(mUfsPath)) {
      System.out.printf("%s is not a valid path", mUfsPath);
      System.exit(1);
    }

    // Set common properties
    mConf.set(PropertyKey.UNDERFS_LISTING_LENGTH, "50");
    mConf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "512B");
    // Increase the buffer time of journal writes to speed up tests
    mConf.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "1sec");

    mUfs = UnderFileSystem.Factory.create(mUfsPath, ufsConf);

    int failedCnt = runCommonOperations();

    if (mUfs.getUnderFSType().equals(S3_IDENTIFIER)) {
      failedCnt += runS3Operations();
    }
    System.out.printf("Tests completed with %d failed.%n", failedCnt);
  }

  /**
   * Runs the tests and return a {@link ValidationTaskResult}.
   *
   * @return a task result for all UFS tests
   * */
  // TODO(jiacheng): Refactor and move this into ValidateHdfsMount
  public ValidationTaskResult runValidationTask() throws IOException {
    Closer closer = Closer.create();
    final ByteArrayOutputStream msgBuf = new ByteArrayOutputStream();
    final ByteArrayOutputStream adviceBuf = new ByteArrayOutputStream();
    PrintStream msgStream = new PrintStream(msgBuf, true);
    PrintStream adviceStream = new PrintStream(adviceBuf, true);
    closer.register(msgStream);
    closer.register(adviceStream);
    closer.register(msgBuf);
    closer.register(adviceBuf);
    try {
      UnderFileSystemConfiguration ufsConf = getUfsConf();
      UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find(mUfsPath, ufsConf);
      // Check if the ufs path is valid
      if (factory == null || !factory.supportsPath(mUfsPath)) {
        msgStream.append(String.format("%s is not a valid path%n", mUfsPath));
        adviceStream.append(String.format("Please validate if %s is a correct path%n", mUfsPath));
        return new ValidationTaskResult(ValidationUtils.State.FAILED, TASK_NAME,
                msgBuf.toString(), adviceBuf.toString());
      }

      // Set common properties
      mConf.set(PropertyKey.UNDERFS_LISTING_LENGTH, "50");
      mConf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "512B");
      // Increase the buffer time of journal writes to speed up tests
      mConf.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "1sec");

      mUfs = UnderFileSystem.Factory.create(mUfsPath, ufsConf);

      int failedCnt = runCommonOperations(msgStream, adviceStream, System.err);

      if (mUfs.getUnderFSType().equals(S3_IDENTIFIER)) {
        failedCnt += runS3Operations(msgStream, adviceStream, System.err);
      }
      msgStream.append(String.format("Tests completed with %d failed.%n", failedCnt));
      ValidationUtils.State state = failedCnt == 0 ? ValidationUtils.State.OK
              : ValidationUtils.State.FAILED;
      if (failedCnt > 0) {
        adviceStream.append("Please check the failed UFS operations from the output.");
      }
      return new ValidationTaskResult(state, TASK_NAME, msgBuf.toString(),
              adviceBuf.toString());
    } catch (Exception e) {
      msgStream.append(ValidationUtils.getErrorInfo(e));
      adviceStream.append("Please resolve the errors from failed UFS operations.");
      return new ValidationTaskResult(ValidationUtils.State.FAILED, TASK_NAME,
              msgBuf.toString(), adviceBuf.toString());
    } finally {
      closer.close();
    }
  }

  private UnderFileSystemConfiguration getUfsConf() {
    return UnderFileSystemConfiguration.defaults(mConf)
        .createMountSpecificConf(mConf.copyProperties().entrySet().stream()
            .filter(entry -> mConf.getSource(entry.getKey()) == Source.SYSTEM_PROPERTY)
            .filter(entry -> mConf.isSet(entry.getKey()) && !entry.getValue().isEmpty())
            .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue)));
  }

  private int runCommonOperations() throws Exception {
    return runCommonOperations(System.out, System.out, System.err);
  }

  private int runCommonOperations(PrintStream msgStream, PrintStream adviceStream,
                                  PrintStream errStream) throws Exception {
    String testDir = createTestDirectory();
    return loadAndRunTests(new UnderFileSystemCommonOperations(mUfsPath, testDir, mUfs, mConf),
            testDir, msgStream, adviceStream, errStream);
  }

  private int runS3Operations() throws Exception {
    return runS3Operations(System.out, System.out, System.err);
  }

  private int runS3Operations(PrintStream msgStream,
                              PrintStream adviceStream, PrintStream errStream) throws Exception {
    mConf.set(PropertyKey.UNDERFS_S3_LIST_OBJECTS_V1, "true");
    mConf.set(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_ENABLED, "true");
    mConf.set(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE, "5MB");
    mConf.set(PropertyKey.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE, "0");

    mUfs = UnderFileSystem.Factory.create(mUfsPath, getUfsConf());

    String testDir = createTestDirectory();
    return loadAndRunTests(new S3ASpecificOperations(testDir, mUfs, mConf),
            testDir, msgStream, adviceStream, errStream);
  }

  private int loadAndRunTests(Object operations, String testDir) throws Exception {
    return loadAndRunTests(operations, testDir, System.out, System.out, System.err);
  }

    /**
     * Loads and runs the tests in the target operations class.
     *
     * @param operations the class that contains the tests to run
     * @param testDir the test directory to run tests against
     * @return the number of failed tests
     */
  private int loadAndRunTests(Object operations, String testDir, PrintStream msgStream,
                              PrintStream adviceStream, PrintStream errStream) throws Exception {
    int failedTestCnt = 0;
    try {
      Class classToRun = operations.getClass();
      Field[] fields = classToRun.getDeclaredFields();
      for (Field field : fields) {
        field.setAccessible(true);
      }
      Method[] tests = classToRun.getDeclaredMethods();
      for (Method test : tests) {
        String testName = test.getName();
        if (testName.endsWith("Test")) {
          if (!(mTestList.isEmpty() || mTestList.contains(testName))) {
            continue;
          }
          msgStream.format("Running test: %s...%n", testName);
          boolean passed = false;
          try {
            test.invoke(operations);
            passed = true;
          } catch (Exception e) {
            if (mUfs.getUnderFSType().equals(S3_IDENTIFIER)) {
              logRelatedS3Operations(test, msgStream);
            }
            msgStream.format("Operation %s failed%n", testName);
            msgStream.format(ValidationUtils.getErrorInfo(e));
            errStream.format("Test %s.%s aborted%n%s%n", test.getClass(), test.getName(), e);
          } finally {
            cleanupUfs(testDir);
            RunTestUtils.printTestStatus(passed, msgStream);
            if (!passed) {
              failedTestCnt++;
            }
          }
        }
      }
      return failedTestCnt;
    } finally {
      mUfs.deleteDirectory(testDir, DeleteOptions.defaults().setRecursive(true));
      mUfs.close();
    }
  }

  /**
   * @return the test directory to run tests against
   */
  private String createTestDirectory() throws IOException {
    String testDir = PathUtils.concatPath(mUfsPath, UUID.randomUUID());
    mUfs.mkdirs(testDir);
    return testDir;
  }

  /**
   * Cleans all the files or sub directories inside the given directory
   * in the under filesystem.
   *
   * @param directory the directory to clean
   */
  private void cleanupUfs(String directory) throws IOException {
    UfsStatus[] statuses = mUfs.listStatus(directory);
    if (statuses == null) {
      LOG.error("Path {} is invalid.", directory);
      return;
    }
    for (UfsStatus status : statuses) {
      if (status instanceof UfsFileStatus) {
        mUfs.deleteFile(PathUtils.concatPath(directory, status.getName()));
      } else {
        mUfs.deleteDirectory(PathUtils.concatPath(directory, status.getName()),
            DeleteOptions.defaults().setRecursive(true));
      }
    }
  }

  /**
   * Logs the S3 operations related to the failed test. This method
   * should only be called when the ufs is S3A.
   *
   * @param test the test to log
   */
  private void logRelatedS3Operations(Method test, PrintStream stream) {
    RelatedS3Operations annotation = test.getAnnotation(RelatedS3Operations.class);
    if (annotation != null) {
      String[] ops = annotation.operations();
      if (ops.length > 0) {
        stream.println("Related S3 operations: " + String.join(", ", ops));
      }
    }
  }

  private static String getHelpMessage() {
    return "Test description:\n"
        + "Test the integration between Alluxio and the under filesystem. "
        + "If the given under filesystem is S3, this test can also be used as "
        + "a S3 compatibility test to test if the target under filesystem can "
        + "fulfill the minimum S3 compatibility requirements in order to "
        + "work well with Alluxio through Alluxio's integration with S3. \n"
        + "Command line example: 'bin/alluxio runUfsTests --path s3://testPath "
        + "-Daws.accessKeyId=<accessKeyId> -Daws.secretKeyId=<secretKeyId>"
        + "-Dalluxio.underfs.s3.endpoint=<endpoint_url> "
        + "-Dalluxio.underfs.s3.disable.dns.buckets=true'";
  }

  /**
   * @param args the input arguments
   */
  public static void main(String[] args) throws Exception {
    UnderFileSystemContractTest test = new UnderFileSystemContractTest();
    JCommander jc = new JCommander(test);
    jc.setProgramName(UnderFileSystemContractTest.class.getName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      jc.usage();
      System.out.println(getHelpMessage());
      System.exit(1);
    }
    if (test.mHelp) {
      jc.usage();
      System.out.println(getHelpMessage());
    } else {
      test.run();
    }
  }
}
