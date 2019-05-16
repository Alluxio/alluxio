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
import alluxio.examples.RelatedS3Operations;
import alluxio.examples.S3ASpecificOperations;
import alluxio.examples.UnderFileSystemCommonOperations;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

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
  private static final String S3_IDENTIFIER = "s3";

  @Parameter(names = {"--path"}, required = true,
      description = "The under filesystem path to run tests against.")
  private String mUfsPath;

  @Parameter(names = {"--help"}, help = true)
  private boolean mHelp = false;

  private InstancedConfiguration mConf
      = new InstancedConfiguration(ConfigurationUtils.defaults());

  // The factory to check if the given ufs path is valid and create ufs
  private UnderFileSystemFactory mFactory;
  private UnderFileSystem mUfs;

  private UnderFileSystemContractTest() {}

  private void run() throws Exception {
    mFactory = UnderFileSystemFactoryRegistry.find(mUfsPath,
        UnderFileSystemConfiguration.defaults(mConf));
    // Check if the ufs path is valid
    if (mFactory == null || !mFactory.supportsPath(mUfsPath)) {
      LOG.error("{} is not a valid path", mUfsPath);
      System.exit(1);
    }

    // Set common properties
    mConf.set(PropertyKey.UNDERFS_LISTING_LENGTH, "50");
    mConf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "512B");
    // Increase the buffer time of journal writes to speed up tests
    mConf.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "1sec");

    createUnderFileSystem();

    runCommonOperations();

    if (mUfs.getUnderFSType().equals(S3_IDENTIFIER)) {
      runS3AOperations();
    }
    CliUtils.printPassInfo(true);
  }

  private void runCommonOperations() throws Exception {
    // Create the test directory to run tests against
    String testDir = PathUtils.concatPath(mUfsPath, UUID.randomUUID());
    loadAndRunTests(new UnderFileSystemCommonOperations(mUfsPath, testDir, mUfs, mConf),
        testDir);
  }

  private void runS3AOperations() throws Exception {
    mConf.set(PropertyKey.UNDERFS_S3A_LIST_OBJECTS_VERSION_1, "true");
    mConf.set(PropertyKey.UNDERFS_S3A_STREAMING_UPLOAD_ENABLED, "true");
    mConf.set(PropertyKey.UNDERFS_S3A_STREAMING_UPLOAD_PARTITION_SIZE, "5MB");
    mConf.set(PropertyKey.UNDERFS_S3A_INTERMEDIATE_UPLOAD_CLEAN_AGE, "0");
    createUnderFileSystem();

    String testDir = PathUtils.concatPath(mUfsPath, UUID.randomUUID());
    loadAndRunTests(new S3ASpecificOperations(testDir, mUfs, mConf), testDir);
  }

  /**
   * Loads and runs the tests in the target operations class.
   *
   * @param operations the class that contains the tests to run
   * @param testDir the test directory to run tests against
   */
  private void loadAndRunTests(Object operations, String testDir) throws Exception {
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
          LOG.info("Running test: " + testName);
          try {
            test.invoke(operations);
          } catch (InvocationTargetException e) {
            if (mUfs.getUnderFSType().equals(S3_IDENTIFIER)) {
              logRelatedS3Operations(test);
            }
            throw new IOException(e.getTargetException());
          }
          LOG.info("Test Passed!");
          cleanupUfs(testDir);
        }
      }
    } finally {
      mUfs.deleteDirectory(testDir, DeleteOptions.defaults().setRecursive(true));
      mUfs.close();
    }
  }

  /**
   * Creates the under file system.
   */
  private void createUnderFileSystem() {
    mUfs = mFactory.create(mUfsPath,
        UnderFileSystemConfiguration.defaults(mConf));
    if (mUfs == null) {
      LOG.error("Failed to create under filesystem");
      System.exit(1);
    }
  }

  /**
   * Cleans all the files or sub directories inside the given directory
   * in the under filesystem.
   *
   * @param directory the directory to clean
   */
  private void cleanupUfs(String directory) throws IOException {
    UfsStatus[] statuses = mUfs.listStatus(directory);
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
  private void logRelatedS3Operations(Method test) {
    RelatedS3Operations annotation = test.getAnnotation(RelatedS3Operations.class);
    if (annotation != null) {
      String[] ops = annotation.operations();
      if (ops.length > 0) {
        LOG.info("Related S3 operations: " + String.join(", ", ops));
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
        + "Command line example: 'bin/alluxio runUfsTest --path=s3a://testPath "
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
      LOG.error(e.getMessage());
      jc.usage();
      LOG.info(getHelpMessage());
      System.exit(1);
    }
    if (test.mHelp) {
      jc.usage();
      LOG.info(getHelpMessage());
    } else {
      test.run();
    }
  }
}
