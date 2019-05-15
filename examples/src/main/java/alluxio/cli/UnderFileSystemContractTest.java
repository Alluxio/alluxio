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
import alluxio.examples.S3ASpecificOperations;
import alluxio.examples.UnderFileSystemCommonOperations;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.s3a.S3AUnderFileSystem;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Integration tests for Alluxio under filesystems. It describes the contract of Alluxio
 * with the UFS through the UFS interface.
 */
public final class UnderFileSystemContractTest {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemContractTest.class);

  @Parameter(names = {"--path"}, required = true,
      description = "The under filesystem path to run tests against.")
  private String mUfsPath;

  @Parameter(names = {"--help"}, help = true)
  private boolean mHelp = false;

  // The factory to check if the given ufs path is valid and create ufs
  private UnderFileSystemFactory mFactory;
  private UnderFileSystem mUfs;
  private String mUfsType;
  private InstancedConfiguration mConf
      = new InstancedConfiguration(ConfigurationUtils.defaults());
  private String mFailedTest;

  private UnderFileSystemContractTest() {}

  private void run() throws Exception {
    mFactory = UnderFileSystemFactoryRegistry.find(mUfsPath,
        UnderFileSystemConfiguration.defaults(mConf));
    // Check if the ufs path is valid
    if (mFactory == null || !mFactory.supportsPath(mUfsPath)) {
      LOG.error("{} is not a valid path", mUfsPath);
      System.exit(1);
    }

    mConf.set(PropertyKey.UNDERFS_LISTING_LENGTH, "50");
    mConf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "512B");
    // Increase the buffer time of journal writes to speed up tests
    mConf.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "1sec");

    mUfs = createUnderFileSystem();
    mUfsType = mUfs.getUnderFSType();

    runCommonOperations();

    if (mUfsType.equals("s3")) {
      runS3AOperations();
    }
    CliUtils.printPassInfo(true);
  }

  private void runCommonOperations() throws Exception {
    // Create the test directory to run tests against
    String testDir = PathUtils.concatPath(mUfsPath, UUID.randomUUID());
    UnderFileSystemCommonOperations ops
        = new UnderFileSystemCommonOperations(mUfsPath, testDir, mUfs, mConf);
    loadAndRunTests(ops, testDir);
  }

  private void runS3AOperations() throws Exception {
    mConf.set(PropertyKey.UNDERFS_S3A_LIST_OBJECTS_VERSION_1, "true");
    mConf.set(PropertyKey.UNDERFS_S3A_STREAMING_UPLOAD_ENABLED, "true");
    mConf.set(PropertyKey.UNDERFS_S3A_STREAMING_UPLOAD_PARTITION_SIZE, "5MB");
    mConf.set(PropertyKey.UNDERFS_S3A_INTERMEDIATE_UPLOAD_CLEAN_AGE, "0");

    mUfs = createUnderFileSystem();
    String testDir = PathUtils.concatPath(mUfsPath, UUID.randomUUID());
    S3ASpecificOperations ops
        = new S3ASpecificOperations(mUfsPath, testDir, (S3AUnderFileSystem) mUfs, mConf);
    loadAndRunTests(ops, testDir);
  }

  private void loadAndRunTests(Object operationsToTest, String testDir) throws Exception {
    try {
      Class classToRun = operationsToTest.getClass();
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
            test.invoke(operationsToTest);
          } catch (InvocationTargetException e) {
            if (mUfsType.equals("S3A")) {
              List<String> operations = getRelatedS3AOperations(testName);
              if (operations.size() > 0) {
                LOG.info("Related S3 operations: "
                    + StringUtils.join(operations, ","));
              }
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

  private UnderFileSystem createUnderFileSystem() {
    UnderFileSystem ufs = mFactory.create(mUfsPath,
        UnderFileSystemConfiguration.defaults(mConf));
    if (ufs == null) {
      LOG.error("Failed to create under filesystem");
      System.exit(1);
    }
    return ufs;
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
   * Gets the S3A operations related to the failed test. This method
   * should only be called when the ufs is S3A.
   *
   * @param testName the name of the failed test
   * @return the related S3A operations
   */
  private List<String> getRelatedS3AOperations(String testName) {
    List<String> operations = new ArrayList<>();
    switch (testName) {
      case "createFileLessThanOnePartTest":
        operations.add("AmazonS3Client.initiateMultipartUpload()");
        operations.add("AmazonS3Client.uploadPart()");
        operations.add("AmazonS3Client.completeMultipartUpload()");
        break;
      case "createMultipartFileTest":
        operations.add("AmazonS3Client.initiateMultipartUpload()");
        operations.add("AmazonS3Client.uploadPart()");
        operations.add("AmazonS3Client.completeMultipartUpload()");
        operations.add("AmazonS3Client.listObjects()");
        break;
      case "createAndAbortMultipartFileTest":
        operations.add("AmazonS3Client.initiateMultipartUpload()");
        operations.add("AmazonS3Client.uploadPart()");
        operations.add("AmazonS3Client.listMultipartUploads()");
        operations.add("TransferManager.abortMultipartUploads()");
        break;
      case "createAtomicTest":
      case "createEmptyTest":
      case "createParentTest":
      case "createThenGetExistingFileStatusTest":
      case "createThenGetExistingStatusTest":
      case "getFileSizeTest":
      case "getFileStatusTest":
      case "getModTimeTest":
        operations.add("TransferManager.upload()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        break;
      case "createOpenTest":
      case "createOpenAtPositionTest":
      case "createOpenEmptyTest":
      case "createOpenExistingLargeFileTest":
      case "createOpenLargeTest":
        operations.add("TransferManager.upload()");
        operations.add("AmazonS3Client.getObject()");
        break;
      case "deleteFileTest":
        operations.add("TransferManager.upload()");
        operations.add("AmazonS3Client.deleteObject()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        break;
      case "deleteDirTest":
      case "deleteLargeDirectoryTest":
      case "createThenDeleteExistingDirectoryTest":
        operations.add("AmazonS3Client.putObject()");
        operations.add("AmazonS3Client.deleteObjects()");
        operations.add("AmazonS3Client.listObjectsV2()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        break;
      case "createDeleteFileConjuctionTest":
        operations.add("TransferManager.upload()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        operations.add("AmazonS3Client.deleteObject()");
        break;
      case "existsTest":
      case "getDirectoryStatusTest":
      case "createThenGetExistingDirectoryStatus":
        operations.add("AmazonS3Client.putObject()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        break;
      case "isFileTest":
        operations.add("AmazonS3Client.putObject()");
        operations.add("AmazonS3Client.deleteObject()");
        break;
      case "listStatusTest":
      case "listStatusEmptyTest":
      case "listStatusFileTest":
      case "listLargeDirectoryTest":
      case "listStatusRecursiveTest":
      case "mkdirsTest":
      case "objectCommonPrefixesIsDirectoryTest":
      case "objectCommonPrefixesListStatusNonRecursiveTest":
      case "objectCommonPrefixesListStatusRecursiveTest":
      case "objectNestedDirsListStatusRecursiveTest":
        operations.add("AmazonS3Client.putObject()");
        operations.add("AmazonS3Client.listObjectsV2()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        break;
      case "renameFileTest":
      case "renameRenamableFileTest":
        operations.add("TransferManager.upload()");
        operations.add("TransferManager.copyObject()");
        operations.add("AmazonS3Client.deleteObject()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        break;
      case "renameDirectoryTest":
      case "renameDirectoryDeepTest":
      case "renameLargeDirectoryTest":
      case "renameRenameableDirectoryTest":
        operations.add("AmazonS3Client.putObject()");
        operations.add("TransferManager.upload()");
        operations.add("TransferManager.copyObject()");
        operations.add("AmazonS3Client.listObjectsV2()");
        operations.add("AmazonS3Client.getObjectMetadata()");
        break;
      default:
        break;
    }
    return operations;
  }

  private static String getHelpMessage() {
    return "Test description:\n"
        + "Test the integration between Alluxio and the under filesystem. "
        + "If the given under filesystem name is S3A, this test can also be used as "
        + "a S3A compatibility test to test if the target under filesystem can "
        + "fulfill the minimum S3A compatibility requirements in order to "
        + "work well with Alluxio through Alluxio's integration with S3A. \n"
        + "Command line example: 'bin/alluxio runUnderFileSystemTest --path=s3a://testPath "
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
