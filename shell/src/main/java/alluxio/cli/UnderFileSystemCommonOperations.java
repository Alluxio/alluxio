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
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Examples for under filesystem common operations.
 * The class should contain all the Alluxio ufs semantics.
 */
public final class UnderFileSystemCommonOperations {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemCommonOperations.class);
  private static final byte[] TEST_BYTES = "TestBytes".getBytes();

  private static final String FILE_CONTENT_LENGTH_INCORRECT
      = "The content length of the written file is %s but expected %s";
  private static final String FILE_CONTENT_INCORRECT
      = "The content of the written file is incorrect";
  private static final String FILE_EXISTS_CHECK_SHOULD_SUCCEED
      = "Should succeed in UnderFileSystem.exists() check, but failed";
  private static final String FILE_EXISTS_CHECK_SHOULD_FAILED
      = "Should failed in UnderFileSystem.exists() check, but succeed";
  private static final String FILE_STATUS_RESULT_INCORRECT
      = "The result of UnderFileSystem.getFileStatus() is incorrect";
  private static final String IS_FAIL_CHECK_SHOULD_SUCCEED
      = "Should succeed in UnderFileSystem.isFile() check, but failed";
  private static final String IS_FAIL_CHECK_SHOULD_FAILED
      = "Should failed in UnderFileSystem.isFile() check, but succeed";
  private static final String IS_DIRECTORY_CHECK_SHOULD_SUCCEED
      = "Should succeed in UnderFileSystem.isDirectory() check, but failed";
  private static final String IS_DIRECTORY_CHECK_SHOULD_FAILED
      = "Should failed in UnderFileSystem.isDirectory() check, but succeed";
  private static final String LIST_STATUS_RESULT_INCORRECT
      = "The result of UnderFileSystem.listStatus() is incorrect";
  private static final int RETRY_TIMEOUT_MS = 180000;
  private static final int RETRY_INTERVAL_MS = 1000;

  private final InstancedConfiguration mConfiguration;
  private final UnderFileSystem mUfs;
  private final String mUfsPath;
  // A child directory of the ufs path to run tests against
  private final String mTopLevelTestDirectory;

  /**
   * @param ufsPath the under filesystem path
   * @param topLevelTestDirectory the top level test directory
   * @param ufs the under filesystem
   * @param configuration the instance configuration
   */
  public UnderFileSystemCommonOperations(String ufsPath, String topLevelTestDirectory,
      UnderFileSystem ufs, InstancedConfiguration configuration) {
    mUfsPath = ufsPath;
    mTopLevelTestDirectory = topLevelTestDirectory;
    mUfs = ufs;
    mConfiguration = configuration;
  }

  /**
   * Test for creating file atomic.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata"})
  public void createAtomicTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createAtomic");
    OutputStream stream = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
        .setEnsureAtomic(true));
    stream.write(TEST_BYTES);
    if (mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
    stream.close();
    if (!mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for creating empty file.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata"})
  public void createEmptyTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createEmpty");
    createEmptyFile(testFile);
    if (!mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for creating file without parent.
   */
  @RelatedS3Operations(operations = {})
  public void createNoParentTest() throws IOException {
    // Run the test only for local UFS. Other UFSs succeed if no parents are present
    if (!UnderFileSystemUtils.isLocal(mUfs)) {
      return;
    }
    boolean haveIOException = false;
    try {
      String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createNoParent/testFile");
      OutputStream o = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
          .setCreateParent(false));
      o.close();
    } catch (IOException e) { // Expected to have IOException
      haveIOException = true;
    }
    if (!haveIOException) {
      throw new IOException("Expected to have IOException but do not have");
    }
  }

  /**
   * Test for creating file with parent.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata"})
  public void createParentTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createParent/testFile");
    OutputStream o = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
        .setCreateParent(true));
    o.close();
    if (!mUfs.exists(testFile)) {
      throw new IOException(FILE_EXISTS_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for creating and opening file.
   */
  @RelatedS3Operations(operations = {"upload", "getObject"})
  public void createOpenTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createOpen");
    createTestBytesFile(testFile);
    byte[] buf = new byte[TEST_BYTES.length];
    int bytesRead = mUfs.open(testFile).read(buf);
    if (TEST_BYTES.length != bytesRead || !Arrays.equals(buf, TEST_BYTES)) {
      throw new IOException(FILE_CONTENT_INCORRECT);
    }
  }

  /**
   * Test for creating and opening empty file.
   */
  @RelatedS3Operations(operations = {"upload", "getObject"})
  public void createOpenEmptyTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createOpenEmpty");
    createEmptyFile(testFile);
    byte[] buf = new byte[0];
    int bytesRead = mUfs.open(testFile).read(buf);
    boolean bytesReadCorrect = bytesRead == 0;
    if (UnderFileSystemUtils.isHdfs(mUfs) && bytesRead == -1) {
      // TODO(adit): Consider making the return value uniform across UFSs
      bytesReadCorrect = true;
    }
    if (!bytesReadCorrect) {
      throw new IOException(String.format(FILE_CONTENT_LENGTH_INCORRECT, bytesRead, 0));
    }
  }

  /**
   * Test for creating file and opening at position.
   */
  @RelatedS3Operations(operations = {"upload", "getObject"})
  public void createOpenAtPositionTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createOpenAtPosition");
    prepareMultiBlockFile(testFile);
    int[] offsets = {0, 256, 511, 512, 513, 768, 1024, 1025};
    for (int offset : offsets) {
      InputStream inputStream = mUfs.open(testFile, OpenOptions.defaults().setOffset(offset));
      if (TEST_BYTES[offset % TEST_BYTES.length] != inputStream.read()) {
        throw new IOException(FILE_CONTENT_INCORRECT);
      }
      inputStream.close();
    }
  }

  /**
   * Test for creating and opening large file.
   */
  @RelatedS3Operations(operations = {"upload", "getObject"})
  public void createOpenLargeTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createOpenLarge");
    int numCopies = prepareMultiBlockFile(testFile);
    InputStream inputStream = mUfs.open(testFile);
    byte[] buf = new byte[numCopies * TEST_BYTES.length];
    int offset = 0;
    int noReadCount = 0;
    while (offset < buf.length && noReadCount < 3) {
      int bytesRead;
      try {
        bytesRead = inputStream.read(buf, offset, buf.length - offset);
      } catch (Exception e) {
        LOG.info("Failed to read from file {}: {}", testFile, e.toString());
        bytesRead = -1;
      }
      if (bytesRead != -1) {
        noReadCount = 0;
        for (int i = 0; i < bytesRead; ++i) {
          if (TEST_BYTES[(offset + i) % TEST_BYTES.length] != buf[offset + i]) {
            throw new IOException(FILE_CONTENT_INCORRECT);
          }
        }
        offset += bytesRead;
      } else {
        ++noReadCount;
      }
    }
    if (noReadCount > 3) {
      throw new IOException("Too many retries in reading the written large file");
    }
  }

  /**
   * Test for creating and open existing large file.
   */
  @RelatedS3Operations(operations = {"upload", "getObject"})
  public void createOpenExistingLargeFileTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createOpenExistingLargeFile");
    int numCopies = prepareMultiBlockFile(testFile);
    InputStream inputStream = mUfs.openExistingFile(testFile);
    byte[] buf = new byte[numCopies * TEST_BYTES.length];
    int offset = 0;
    while (offset < buf.length) {
      int bytesRead = inputStream.read(buf, offset, buf.length - offset);
      if (bytesRead == -1) {
        break;
      }
      for (int i = 0; i < bytesRead; ++i) {
        if (TEST_BYTES[(offset + i) % TEST_BYTES.length] != buf[offset + i]) {
          throw new IOException(FILE_CONTENT_INCORRECT);
        }
      }
      offset += bytesRead;
    }
    if (buf.length != offset) {
      throw new IOException(FILE_CONTENT_INCORRECT);
    }
  }

  /**
   * Test for create file, open and seek.
   */
  @RelatedS3Operations(operations = {"upload", "getObject"})
  public void createOpenSkip() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "createOpenSkip");
    prepareMultiBlockFile(testFile);
    int[] offsets = {0, 256, 511, 512, 513, 768, 1024, 1025};
    for (int offset : offsets) {
      InputStream inputStream = mUfs.open(testFile, OpenOptions.defaults());
      long bytesSkipped = 0;
      while (bytesSkipped != offset) {
        bytesSkipped += inputStream.skip(offset - bytesSkipped);
      }
      if (TEST_BYTES[offset % TEST_BYTES.length] != inputStream.read()) {
        throw new IOException(FILE_CONTENT_INCORRECT);
      }
      inputStream.close();
    }
  }

  /**
   * Test for deleting file.
   */
  @RelatedS3Operations(operations = {"upload", "deleteObject", "getObjectMetadata"})
  public void deleteFileTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "deleteFile");
    createEmptyFile(testFile);
    mUfs.deleteFile(testFile);
    if (mUfs.exists(testFile)) {
      throw new IOException(FILE_EXISTS_CHECK_SHOULD_FAILED);
    }
    if (mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
  }

  /**
   * Test for deleting directory.
   */
  @RelatedS3Operations(operations = {"putObject", "deleteObjects",
      "listObjectsV2", "getObjectMetadata"})
  public void deleteDirTest() throws IOException {
    String testDirEmpty = PathUtils.concatPath(mTopLevelTestDirectory, "deleteDirTestDirEmpty");
    String testDirNonEmpty = PathUtils
        .concatPath(mTopLevelTestDirectory, "deleteDirTestDirNonEmpty1");
    String testDirNonEmptyChildDir
        = PathUtils.concatPath(testDirNonEmpty, "deleteDirTestDirNonEmpty2");
    String testDirNonEmptyChildFile
        = PathUtils.concatPath(testDirNonEmpty, "deleteDirTestDirNonEmptyF");
    String testDirNonEmptyChildDirFile =
        PathUtils.concatPath(testDirNonEmptyChildDir, "deleteDirTestDirNonEmptyChildDirF");
    mUfs.mkdirs(testDirEmpty, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(testDirNonEmpty, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(testDirNonEmptyChildDir, MkdirsOptions.defaults(mConfiguration)
        .setCreateParent(false));
    createEmptyFile(testDirNonEmptyChildFile);
    createEmptyFile(testDirNonEmptyChildDirFile);
    mUfs.deleteDirectory(testDirEmpty, DeleteOptions.defaults().setRecursive(false));
    if (mUfs.isDirectory(testDirEmpty)) {
      throw new IOException("Directory is deleted "
          + "but succeed in UnderFileSystem.isDirectory() check");
    }
    try {
      mUfs.deleteDirectory(testDirNonEmpty, DeleteOptions.defaults().setRecursive(false));
    } catch (IOException e) {
      // Some File systems may throw IOException
    }
    if (!mUfs.isDirectory(testDirNonEmpty)) {
      throw new IOException("Created directory should succeed "
          + "in UnderFileSystem.isDirectory() check, but failed");
    }
    mUfs.deleteDirectory(testDirNonEmpty, DeleteOptions.defaults().setRecursive(true));
    if (mUfs.isDirectory(testDirNonEmpty) || mUfs.isDirectory(testDirNonEmptyChildDir)
        || mUfs.isFile(testDirNonEmptyChildFile) || mUfs.isFile(testDirNonEmptyChildDirFile)) {
      throw new IOException("Deleted file or directory still exist");
    }
  }

  /**
   * Test for deleting large directory.
   */
  @RelatedS3Operations(operations = {"putObject", "deleteObjects",
      "listObjectsV2", "getObjectMetadata"})
  public void deleteLargeDirectoryTest() throws Exception {
    LargeDirectoryConfig config = prepareLargeDirectory();
    mUfs.deleteExistingDirectory(config.getTopLevelDirectory(),
        DeleteOptions.defaults().setRecursive(true));

    String[] children = config.getChildren();
    for (String child : children) {
      // Retry for some time to allow list operations eventual consistency for S3 and GCS.
      // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
      // https://cloud.google.com/storage/docs/consistency for more details.
      CommonUtils.waitFor("deleted path does not exist", () -> {
        try {
          return !mUfs.isFile(child) && !mUfs.isDirectory(child);
        } catch (Exception e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(RETRY_TIMEOUT_MS).setInterval(RETRY_INTERVAL_MS));
    }
  }

  /**
   * Test for creating and deleting file conjunction.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata", "deleteObject"})
  public void createDeleteFileConjuctionTest() throws IOException {
    String testFile = PathUtils
        .concatPath(mTopLevelTestDirectory, "deleteThenCreateNonexistingFile");
    createTestBytesFile(testFile);
    if (!mUfs.exists(testFile)) {
      throw new IOException(FILE_EXISTS_CHECK_SHOULD_SUCCEED);
    }
    if (!mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }

    mUfs.deleteExistingFile(testFile);
    if (mUfs.exists(testFile)) {
      throw new IOException(FILE_EXISTS_CHECK_SHOULD_FAILED);
    }

    OutputStream o = mUfs.createNonexistingFile(testFile);
    o.write(TEST_BYTES);
    o.close();
    if (!mUfs.exists(testFile)) {
      throw new IOException(FILE_EXISTS_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for creating and deleting existing directory.
   */
  @RelatedS3Operations(operations = {"putObject", "deleteObjects",
      "listObjectsV2", "getObjectMetadata"})
  public void createThenDeleteExistingDirectoryTest() throws IOException {
    LargeDirectoryConfig config = prepareLargeDirectory();
    if (!mUfs.deleteExistingDirectory(config.getTopLevelDirectory(),
        DeleteOptions.defaults().setRecursive(true))) {
      throw new IOException("Failed to delete existing directory");
    }
  }

  /**
   * Test for checking file existence.
   */
  @RelatedS3Operations(operations = {"putObject", "getObjectMetadata"})
  public void existsTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "testFile");
    if (mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
    createEmptyFile(testFile);
    if (!mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
    String testDir = PathUtils.concatPath(mTopLevelTestDirectory, "testDir");
    if (mUfs.isDirectory(testDir)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_FAILED);
    }
    mUfs.mkdirs(testDir, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    if (!mUfs.isDirectory(testDir)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for getting directory status.
   */
  @RelatedS3Operations(operations = {"putObject", "getObjectMetadata"})
  public void getDirectoryStatusTest() throws IOException {
    String testDir = PathUtils.concatPath(mTopLevelTestDirectory, "testDir");
    mUfs.mkdirs(testDir);

    UfsStatus status = mUfs.getStatus(testDir);
    if (!(status instanceof UfsDirectoryStatus)) {
      throw new IOException("Failed to get ufs directory status");
    }
  }

  /**
   * Test for getting existing directory status.
   */
  @RelatedS3Operations(operations = {"putObject", "getObjectMetadata"})
  public void createThenGetExistingDirectoryStatusTest() throws IOException {
    String testDir = PathUtils.concatPath(mTopLevelTestDirectory, "testDir");
    mUfs.mkdirs(testDir);
    UfsStatus status = mUfs.getExistingStatus(testDir);
    if (!(status instanceof UfsDirectoryStatus)) {
      throw new IOException("Failed to get ufs directory status");
    }
  }

  /**
   * Test for getting file size.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata"})
  public void getFileSizeTest() throws IOException {
    String testFileEmpty = PathUtils.concatPath(mTopLevelTestDirectory, "testFileEmpty");
    String testFileNonEmpty = PathUtils.concatPath(mTopLevelTestDirectory, "testFileNonEmpty");
    createEmptyFile(testFileEmpty);
    createTestBytesFile(testFileNonEmpty);
    if (mUfs.getFileStatus(testFileEmpty).getContentLength() != 0
        || mUfs.getFileStatus(testFileNonEmpty).getContentLength() != TEST_BYTES.length) {
      throw new IOException(FILE_STATUS_RESULT_INCORRECT);
    }
  }

  /**
   * Test for getting existing file status.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata"})
  public void createThenGetExistingFileStatusTest() throws IOException {
    String testFileNonEmpty = PathUtils.concatPath(mTopLevelTestDirectory, "testFileNonEmpty");
    String testFileLarge = PathUtils.concatPath(mTopLevelTestDirectory, "testFileLarge");
    createTestBytesFile(testFileNonEmpty);
    int numCopies = prepareMultiBlockFile(testFileLarge);
    if (TEST_BYTES.length != mUfs.getExistingFileStatus(testFileNonEmpty).getContentLength()
        || TEST_BYTES.length * numCopies
        != mUfs.getExistingFileStatus(testFileLarge).getContentLength()) {
      throw new IOException(FILE_STATUS_RESULT_INCORRECT);
    }
  }

  /**
   * Test for getting file status.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata"})
  public void getFileStatusTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "testFile");
    createEmptyFile(testFile);

    UfsStatus status = mUfs.getStatus(testFile);
    if (!(status instanceof UfsFileStatus)) {
      throw new IOException("Failed to get ufs file status");
    }
  }

  /**
   * Test for getting existing status.
   */
  @RelatedS3Operations(operations = {})
  public void createThenGetExistingStatusTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "testFile");
    createTestBytesFile(testFile);

    UfsStatus status = mUfs.getExistingStatus(testFile);
    if (!(status instanceof UfsFileStatus)) {
      throw new IOException("Failed to get ufs file status");
    }
  }

  /**
   * Test for getting modification time.
   */
  @RelatedS3Operations(operations = {"upload", "getObjectMetadata"})
  public void getModTimeTest() throws IOException {
    long slack = 5000; // Some file systems may report nearest second.
    long start = System.currentTimeMillis();
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "testFile");
    createTestBytesFile(testFile);
    long end = System.currentTimeMillis();
    long modTime = mUfs.getFileStatus(testFile).getLastModifiedTime();
    if (modTime < start - slack || modTime > end + slack) {
      throw new IOException(FILE_STATUS_RESULT_INCORRECT);
    }
  }

  /**
   * Test for getting status of non-existent directory.
   */
  @RelatedS3Operations(operations = {"getObjectMetadata"})
  public void getNonExistingDirectoryStatusTest() throws IOException {
    String testDir = PathUtils.concatPath(mTopLevelTestDirectory, "nonExistentDir");
    try {
      mUfs.getDirectoryStatus(testDir);
    } catch (FileNotFoundException e) {
      return;
    }
    throw new IOException(
        "Get status on a non-existent directory did not through " + FileNotFoundException.class);
  }

  /**
   * Test for getting status of non-existent file.
   */
  @RelatedS3Operations(operations = {"getObjectMetadata"})
  public void getNonExistingFileStatusTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "nonExistentFile");
    try {
      mUfs.getFileStatus(testFile);
    } catch (FileNotFoundException e) {
      return;
    }
    throw new IOException(
        "Get file status on a non-existent file did not through " + FileNotFoundException.class);
  }

  /**
   * Test for getting status of non-existent path.
   */
  @RelatedS3Operations(operations = {"getObjectMetadata"})
  public void getNonExistingPathStatusTest() throws IOException {
    String testPath = PathUtils.concatPath(mTopLevelTestDirectory, "nonExistentPath");
    try {
      mUfs.getStatus(testPath);
    } catch (FileNotFoundException e) {
      return;
    }
    throw new IOException(
        "Get status on a non-existent path did not through " + FileNotFoundException.class);
  }

  /**
   * Test for checking file is actual file.
   */
  @RelatedS3Operations(operations = {"putObject", "deleteObject"})
  public void isFileTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "testFile");
    String testDir = PathUtils.concatPath(mTopLevelTestDirectory, "testDir");
    if (mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
    createEmptyFile(testFile);
    mUfs.mkdirs(testDir, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    if (!mUfs.isFile(testFile)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
    if (mUfs.isFile(testDir)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
  }

  /**
   * Test for listing status.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void listStatusTest() throws IOException {
    String testDirNonEmpty = PathUtils.concatPath(mTopLevelTestDirectory, "testDirNonEmpty1");
    String testDirNonEmptyChildDir = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmpty2");
    String testDirNonEmptyChildFile = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmptyF");
    String testDirNonEmptyChildDirFile =
        PathUtils.concatPath(testDirNonEmptyChildDir, "testDirNonEmptyChildDirF");
    mUfs.mkdirs(testDirNonEmpty, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(testDirNonEmptyChildDir, MkdirsOptions.defaults(mConfiguration)
        .setCreateParent(false));
    createEmptyFile(testDirNonEmptyChildFile);
    createEmptyFile(testDirNonEmptyChildDirFile);
    String[] expectedResTopDir = new String[] {"testDirNonEmpty2", "testDirNonEmptyF"};
    // Some file systems may prefix with a slash
    String[] expectedResTopDir2 = new String[] {"/testDirNonEmpty2", "/testDirNonEmptyF"};
    Arrays.sort(expectedResTopDir);
    Arrays.sort(expectedResTopDir2);
    UfsStatus[] resTopDirStatus = mUfs.listStatus(testDirNonEmpty);
    String[] resTopDir = UfsStatus.convertToNames(resTopDirStatus);
    Arrays.sort(resTopDir);
    if (!Arrays.equals(expectedResTopDir, resTopDir)
        && !Arrays.equals(expectedResTopDir2, resTopDir)) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
    if (!mUfs.listStatus(testDirNonEmptyChildDir)[0].getName().equals("testDirNonEmptyChildDirF")
            || mUfs.listStatus(testDirNonEmptyChildDir)[0].getName()
            .equals("/testDirNonEmptyChildDirF")) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
    for (int i = 0; i < resTopDir.length; ++i) {
      if (mUfs.isDirectory(PathUtils.concatPath(testDirNonEmpty, resTopDirStatus[i].getName()))
          != resTopDirStatus[i].isDirectory()) {
        throw new IOException("UnderFileSystem.isDirectory() result is different from expected");
      }
    }
  }

  /**
   * Test for listing empty directory.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void listStatusEmptyTest() throws IOException {
    String testDir = PathUtils.concatPath(mTopLevelTestDirectory, "listStatusEmpty");
    mUfs.mkdirs(testDir);
    UfsStatus[] res = mUfs.listStatus(testDir);
    if (res.length != 0) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
  }

  /**
   * Test for listing status on file.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void listStatusFileTest() throws IOException {
    String testFile = PathUtils.concatPath(mTopLevelTestDirectory, "listStatusFile");
    createEmptyFile(testFile);
    if (mUfs.listStatus(testFile) != null) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
  }

  /**
   * Test for listing large directory.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void listLargeDirectoryTest() throws Exception {
    LargeDirectoryConfig config = prepareLargeDirectory();
    String[] children = config.getChildren();

    // Retry for some time to allow list operations eventual consistency for S3 and GCS.
    // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
    // https://cloud.google.com/storage/docs/consistency for more details.
    AtomicReference<UfsStatus[]> results = new AtomicReference<>();
    CommonUtils.waitFor("list large directory", () -> {
      try {
        results.set(mUfs.listStatus(config.getTopLevelDirectory()));
        return children.length == results.get().length;
      } catch (Exception e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(RETRY_TIMEOUT_MS).setInterval(RETRY_INTERVAL_MS));

    String[] resultNames = UfsStatus.convertToNames(results.get());
    Arrays.sort(resultNames);
    for (int i = 0; i < children.length; ++i) {
      if (!resultNames[i].equals(CommonUtils.stripPrefixIfPresent(children[i],
          PathUtils.normalizePath(config.getTopLevelDirectory(), "/")))) {
        throw new IOException(LIST_STATUS_RESULT_INCORRECT);
      }
    }
  }

  /**
   * Test for listing status recursively.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void listStatusRecursiveTest() throws IOException {
    String root = mTopLevelTestDirectory;
    // TODO(andrew): Should this directory be created in LocalAlluxioCluster creation code?
    mUfs.mkdirs(root);
    // Empty lsr should be empty
    if (mUfs.listStatus(root).length != 0) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }

    // Create a tree of subdirectories and files
    String sub1 = PathUtils.concatPath(root, "sub1");
    String sub2 = PathUtils.concatPath(root, "sub2");
    String sub11 = PathUtils.concatPath(sub1, "sub11");
    String file11 = PathUtils.concatPath(sub11, "file11");
    String file2 = PathUtils.concatPath(sub2, "file2");
    String file = PathUtils.concatPath(root, "file");
    // lsr of nonexistent path should be null
    if (mUfs.listStatus(sub1, ListOptions.defaults().setRecursive(true)) != null) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }

    mUfs.mkdirs(sub1, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(sub2, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(sub11, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    createEmptyFile(file11);
    createEmptyFile(file2);
    createEmptyFile(file);

    // lsr from root should return paths relative to the root
    String[] expectedResRoot =
        {"sub1", "sub2", "sub1/sub11", "sub1/sub11/file11", "sub2/file2", "file"};
    UfsStatus[] actualResRootStatus =
        mUfs.listStatus(root, ListOptions.defaults().setRecursive(true));
    String[] actualResRoot = UfsStatus.convertToNames(actualResRootStatus);
    Arrays.sort(expectedResRoot);
    Arrays.sort(actualResRoot);
    if (!Arrays.equals(expectedResRoot, actualResRoot)) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
    for (int i = 0; i < actualResRoot.length; ++i) {
      if (mUfs.isDirectory(PathUtils.concatPath(root, actualResRootStatus[i].getName()))
          != actualResRootStatus[i].isDirectory()) {
        throw new IOException("UnderFileSystem.isDirectory() result is different from expected");
      }
    }
    // lsr from sub1 should return paths relative to sub1
    String[] expectedResSub1 = {"sub11", "sub11/file11"};
    Arrays.sort(expectedResSub1);
    // Case A: the path sub1 does not end with a trailing /
    String[] actualResSub1a =
        UfsStatus.convertToNames(mUfs.listStatus(sub1, ListOptions.defaults().setRecursive(true)));
    Arrays.sort(actualResSub1a);
    if (!Arrays.equals(expectedResSub1, actualResSub1a)
        || (mUfs.listStatus(file, ListOptions.defaults().setRecursive(true)) != null)) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
    // Case B: the path sub1 ends with a trailing /
    String[] actualResSub1b = UfsStatus.convertToNames(
        mUfs.listStatus(sub1 + "/", ListOptions.defaults().setRecursive(true)));
    Arrays.sort(actualResSub1b);
    if (!Arrays.equals(expectedResSub1, actualResSub1b)
        || (mUfs.listStatus(file, ListOptions.defaults().setRecursive(true)) != null)) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
  }

  /**
   * Test for creating directory.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void mkdirsTest() throws IOException {
    // make sure the underfs address dir exists already
    mUfs.mkdirs(mTopLevelTestDirectory);
    // empty lsr should be empty
    if (mUfs.listStatus(mTopLevelTestDirectory).length != 0) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }

    String testDirTop = PathUtils.concatPath(mTopLevelTestDirectory, "testDirTop");
    String testDir1 = PathUtils.concatPath(mTopLevelTestDirectory, "1");
    String testDir2 = PathUtils.concatPath(testDir1, "2");
    String testDir3 = PathUtils.concatPath(testDir2, "3");
    String testDirDeep = PathUtils.concatPath(testDir3, "testDirDeep");
    mUfs.mkdirs(testDirTop, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(testDirDeep, MkdirsOptions.defaults(mConfiguration).setCreateParent(true));
    if (!(mUfs.isDirectory(testDirTop) && mUfs.isDirectory(testDir1) && mUfs.isDirectory(testDir2)
        && mUfs.isDirectory(testDir3) && mUfs.isDirectory(testDirDeep))) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for checking directory in object storage.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void objectCommonPrefixesIsDirectoryTest() throws IOException {
    // Only run test for an object store
    if (!mUfs.isObjectStorage()) {
      return;
    }

    ObjectStorePreConfig config = prepareObjectStore();

    String baseDirectoryPath = config.getBaseDirectoryPath();
    if (!mUfs.isDirectory(baseDirectoryPath)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_SUCCEED);
    }

    for (String subDirName : config.getSubDirectoryNames()) {
      String subDirPath = PathUtils.concatPath(baseDirectoryPath, subDirName);
      if (!mUfs.isDirectory(subDirPath)) {
        throw new IOException(IS_DIRECTORY_CHECK_SHOULD_SUCCEED);
      }
    }
  }

  /**
   * Test for listing status non recursively in object storage.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void objectCommonPrefixesListStatusNonRecursiveTest() throws IOException {
    // Only run test for an object store
    if (!mUfs.isObjectStorage()) {
      return;
    }

    ObjectStorePreConfig config = prepareObjectStore();

    String baseDirectoryPath = config.getBaseDirectoryPath();
    UfsStatus[] results = mUfs.listStatus(baseDirectoryPath);
    if (config.getSubDirectoryNames().length + config.getFileNames().length != results.length) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
    // Check for direct children files
    for (String fileName : config.getFileNames()) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(fileName)) {
          foundIndex = i;
        }
      }
      if (foundIndex < 0 || !results[foundIndex].isFile()) {
        throw new IOException(LIST_STATUS_RESULT_INCORRECT);
      }
    }
    // Check if pseudo-directories were inferred
    for (String subDirName : config.getSubDirectoryNames()) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(subDirName)) {
          foundIndex = i;
        }
      }
      if (foundIndex < 0 || !results[foundIndex].isDirectory()) {
        throw new IOException(LIST_STATUS_RESULT_INCORRECT);
      }
    }
  }

  /**
   * Test for listing status recursively in object storage.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void objectCommonPrefixesListStatusRecursiveTest() throws IOException {
    // Only run test for an object store
    if (!mUfs.isObjectStorage()) {
      return;
    }

    ObjectStorePreConfig config = prepareObjectStore();

    String baseDirectoryPath = config.getBaseDirectoryPath();
    UfsStatus[] results =
        mUfs.listStatus(baseDirectoryPath, ListOptions.defaults().setRecursive(true));
    String[] fileNames = config.getFileNames();
    String[] subDirNames = config.getSubDirectoryNames();
    if (subDirNames.length + fileNames.length + subDirNames.length * fileNames.length
        != results.length) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
    // Check for direct children files
    for (String fileName : fileNames) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(fileName)) {
          foundIndex = i;
        }
      }
      if (foundIndex < 0 || !results[foundIndex].isFile()) {
        throw new IOException(LIST_STATUS_RESULT_INCORRECT);
      }
    }
    for (String subDirName : subDirNames) {
      // Check if pseudo-directories were inferred
      int dirIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(subDirName)) {
          dirIndex = i;
        }
      }
      if (dirIndex < 0 || !results[dirIndex].isDirectory()) {
        throw new IOException(LIST_STATUS_RESULT_INCORRECT);
      }
      // Check for indirect children
      for (String fileName : config.getFileNames()) {
        int fileIndex = -1;
        for (int i = 0; i < results.length; ++i) {
          if (results[i].getName().equals(String.format("%s/%s", subDirName, fileName))) {
            fileIndex = i;
          }
        }
        if (fileIndex < 0 || !results[fileIndex].isFile()) {
          throw new IOException(LIST_STATUS_RESULT_INCORRECT);
        }
      }
    }
  }

  /**
   * Test for listing status recursively in nested directory in object storage.
   */
  @RelatedS3Operations(operations = {"putObject", "listObjectsV2", "getObjectMetadata"})
  public void objectNestedDirsListStatusRecursiveTest() throws IOException {
    // Only run test for an object store
    if (!mUfs.isObjectStorage()) {
      return;
    }

    String root = mTopLevelTestDirectory;
    int nesting = 5;

    String path = root;
    for (int i = 0; i < nesting; i++) {
      path = PathUtils.concatPath(path, "dir" + i);
    }
    String file1 = PathUtils.concatPath(path, "file.txt");

    // Empty lsr should be empty
    if (mUfs.listStatus(root, ListOptions.defaults().setRecursive(true)).length != 0) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }

    String fileKey = file1.substring(PathUtils.normalizePath(mUfsPath, "/").length());
    if (!mUfs.mkdirs(fileKey)) {
      throw new IOException("Failed to create empty object");
    }

    path = "";
    ArrayList<String> paths = new ArrayList<>();
    for (int i = 0; i < nesting; i++) {
      if (i == 0) {
        path = "dir" + i;
      } else {
        path = PathUtils.concatPath(path, "dir" + i);
      }
      paths.add(path);
    }
    path = PathUtils.concatPath(path, "file.txt");
    paths.add(path);

    String[] expectedStatus = paths.toArray(new String[paths.size()]);
    String[] actualStatus =
        UfsStatus.convertToNames(mUfs.listStatus(root, ListOptions.defaults().setRecursive(true)));
    Arrays.sort(expectedStatus);
    Arrays.sort(actualStatus);
    if (expectedStatus.length != actualStatus.length
        || !Arrays.equals(expectedStatus, actualStatus)) {
      throw new IOException(LIST_STATUS_RESULT_INCORRECT);
    }
  }

  /**
   * Test for renaming file.
   */
  @RelatedS3Operations(operations = {"upload", "copyObject", "deleteObject", "getObjectMetadata"})
  public void renameFileTest() throws IOException {
    String testFileSrc = PathUtils.concatPath(mTopLevelTestDirectory, "renameFileSrc");
    String testFileDst = PathUtils.concatPath(mTopLevelTestDirectory, "renameFileDst");
    createEmptyFile(testFileSrc);
    mUfs.renameFile(testFileSrc, testFileDst);
    if (mUfs.isFile(testFileSrc)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
    if (!mUfs.isFile(testFileDst)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for renaming renamable file.
   */
  @RelatedS3Operations(operations = {"upload", "copyObject", "deleteObject", "getObjectMetadata"})
  public void renameRenamableFileTest() throws IOException {
    String testFileSrc = PathUtils.concatPath(mTopLevelTestDirectory, "renameFileSrc");
    String testFileDst = PathUtils.concatPath(mTopLevelTestDirectory, "renameFileDst");
    prepareMultiBlockFile(testFileSrc);
    mUfs.renameRenamableFile(testFileSrc, testFileDst);
    if (mUfs.isFile(testFileSrc)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
    if (!mUfs.isFile(testFileDst)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for renaming directory.
   */
  @RelatedS3Operations(operations = {"putObject", "upload", "copyObject",
      "listObjectsV2", "getObjectMetadata"})
  public void renameDirectoryTest() throws IOException {
    String testDirSrc = PathUtils.concatPath(mTopLevelTestDirectory, "renameDirectorySrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirDst = PathUtils.concatPath(mTopLevelTestDirectory, "renameDirectoryDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    createEmptyFile(testDirSrcChild);
    mUfs.renameDirectory(testDirSrc, testDirDst);
    if (mUfs.isDirectory(testDirSrc)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_FAILED);
    }
    if (mUfs.isFile(testDirSrcChild)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }
    if (!mUfs.isDirectory(testDirDst)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_SUCCEED);
    }
    if (!mUfs.isFile(testDirDstChild)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for renaming deep directory.
   */
  @RelatedS3Operations(operations = {"putObject", "upload", "copyObject",
      "listObjectsV2", "getObjectMetadata"})
  public void renameDirectoryDeepTest() throws IOException {
    String testDirSrc = PathUtils.concatPath(mTopLevelTestDirectory, "renameDirectoryDeepSrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirSrcNested = PathUtils.concatPath(testDirSrc, "testNested");
    String testDirSrcNestedChild = PathUtils.concatPath(testDirSrcNested, "testNestedFile");

    String testDirDst = PathUtils.concatPath(mTopLevelTestDirectory, "renameDirectoryDeepDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    String testDirDstNested = PathUtils.concatPath(testDirDst, "testNested");
    String testDirDstNestedChild = PathUtils.concatPath(testDirDstNested, "testNestedFile");

    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    createEmptyFile(testDirSrcChild);
    mUfs.mkdirs(testDirSrcNested, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    createEmptyFile(testDirSrcNestedChild);

    mUfs.renameDirectory(testDirSrc, testDirDst);

    if (mUfs.isDirectory(testDirSrc) || mUfs.isDirectory(testDirSrcNested)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_FAILED);
    }
    if (mUfs.isFile(testDirSrcChild) || mUfs.isFile(testDirSrcNestedChild)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }

    if (!mUfs.isDirectory(testDirDst) || !mUfs.isDirectory(testDirDstNested)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_SUCCEED);
    }
    if (!mUfs.isFile(testDirDstChild) || !mUfs.isFile(testDirDstNestedChild)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for renaming renamable directory.
   */
  @RelatedS3Operations(operations = {"putObject", "upload", "copyObject",
      "listObjectsV2", "getObjectMetadata"})
  public void renameRenamableDirectoryTest() throws IOException {
    String testDirSrc = PathUtils.concatPath(mTopLevelTestDirectory, "renameRenamableDirectorySrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirSrcNested = PathUtils.concatPath(testDirSrc, "testNested");
    String testDirSrcNestedChild = PathUtils.concatPath(testDirSrcNested, "testNestedFile");

    String testDirDst = PathUtils.concatPath(mTopLevelTestDirectory, "renameRenamableDirectoryDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    String testDirDstNested = PathUtils.concatPath(testDirDst, "testNested");
    String testDirDstNestedChild = PathUtils.concatPath(testDirDstNested, "testNestedFile");

    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    prepareMultiBlockFile(testDirSrcChild);
    mUfs.mkdirs(testDirSrcNested, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    prepareMultiBlockFile(testDirSrcNestedChild);

    mUfs.renameRenamableDirectory(testDirSrc, testDirDst);

    if (mUfs.isDirectory(testDirSrc) || mUfs.isDirectory(testDirSrcNested)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_FAILED);
    }
    if (mUfs.isFile(testDirSrcChild) || mUfs.isFile(testDirSrcNestedChild)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_FAILED);
    }

    if (!mUfs.isDirectory(testDirDst) || !mUfs.isDirectory(testDirDstNested)) {
      throw new IOException(IS_DIRECTORY_CHECK_SHOULD_SUCCEED);
    }
    if (!mUfs.isFile(testDirDstChild) || !mUfs.isFile(testDirDstNestedChild)) {
      throw new IOException(IS_FAIL_CHECK_SHOULD_SUCCEED);
    }
  }

  /**
   * Test for renaming large directory.
   */
  @RelatedS3Operations(operations = {"putObject", "upload", "copyObject",
      "listObjectsV2", "getObjectMetadata"})
  public void renameLargeDirectoryTest() throws Exception {
    LargeDirectoryConfig config = prepareLargeDirectory();
    String dstTopLevelDirectory = PathUtils.concatPath(mTopLevelTestDirectory, "topLevelDirMoved");
    mUfs.renameDirectory(config.getTopLevelDirectory(), dstTopLevelDirectory);
    // 1. Check the src directory no longer exists
    String[] srcChildren = config.getChildren();
    for (String src : srcChildren) {
      // Retry for some time to allow list operations eventual consistency for S3 and GCS.
      // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
      // https://cloud.google.com/storage/docs/consistency for more details.
      CommonUtils.waitFor("list after delete consistency", () -> {
        try {
          return !mUfs.exists(src);
        } catch (IOException e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(RETRY_TIMEOUT_MS).setInterval(RETRY_INTERVAL_MS));
    }
    // 2. Check the dst directory exists
    CommonUtils.waitFor("list after create consistency", () -> {
      UfsStatus[] results;
      try {
        results = mUfs.listStatus(dstTopLevelDirectory);
        if (srcChildren.length != results.length) {
          return false;
        }
        // Check nested files and directories in dst exist
        String[] resultNames = UfsStatus.convertToNames(results);
        Arrays.sort(resultNames);
        for (int i = 0; i < srcChildren.length; ++i) {
          if (!resultNames[i].equals(CommonUtils.stripPrefixIfPresent(srcChildren[i],
              PathUtils.normalizePath(config.getTopLevelDirectory(), "/")))) {
            throw new IOException("Destination is different from source after rename directory");
          }
        }
      } catch (IOException e) {
        return false;
      }
      return true;
    }, WaitForOptions.defaults().setTimeoutMs(RETRY_TIMEOUT_MS).setInterval(RETRY_INTERVAL_MS));
  }

  private void createEmptyFile(String path) throws IOException {
    OutputStream o = mUfs.create(path);
    o.close();
  }

  private void createTestBytesFile(String path) throws IOException {
    OutputStream o = mUfs.create(path);
    o.write(TEST_BYTES);
    o.close();
  }

  // Prepare directory tree for pagination tests
  private LargeDirectoryConfig prepareLargeDirectory() throws IOException {
    final String filePrefix = "a_";
    final String folderPrefix = "b_";

    String topLevelDirectory = PathUtils.concatPath(mTopLevelTestDirectory, "topLevelDir");

    final int numFiles = 100;

    String[] children = new String[numFiles + numFiles];

    // Make top level directory
    mUfs.mkdirs(topLevelDirectory, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));

    // Make the children files
    for (int i = 0; i < numFiles; ++i) {
      children[i] = PathUtils.concatPath(topLevelDirectory, filePrefix + String.format("%04d", i));
      createEmptyFile(children[i]);
    }
    // Make the children folders
    for (int i = 0; i < numFiles; ++i) {
      children[numFiles + i] =
          PathUtils.concatPath(topLevelDirectory, folderPrefix + String.format("%04d", i));
      mUfs.mkdirs(children[numFiles + i], MkdirsOptions.defaults(mConfiguration)
          .setCreateParent(false));
    }

    return new LargeDirectoryConfig(topLevelDirectory, children);
  }

  /**
   * Test configuration for pagination tests.
   */
  private class LargeDirectoryConfig {
    private String mTopLevelDirectory;
    // Children for top level directory
    private String[] mChildren;

    /**
     * Constructs {@link LargeDirectoryConfig} for pagination tests.
     *
     * @param topLevelDirectory the top level directory of the directory tree for pagination tests
     * @param children the children files of the directory tree for pagination tests
     */
    LargeDirectoryConfig(String topLevelDirectory, String[] children) {
      mTopLevelDirectory = topLevelDirectory;
      mChildren = children;
    }

    /**
     * @return the top level directory of the directory tree for pagination tests
     */
    String getTopLevelDirectory() {
      return mTopLevelDirectory;
    }

    /**
     * @return the children files of the directory tree for pagination tests
     */
    String[] getChildren() {
      return mChildren;
    }
  }

  /**
   * Prepares a multi-block file by making copies of TEST_BYTES.
   *
   * @param testFile path of file to create
   * @return the number of copies of TEST_BYTES made
   */
  private int prepareMultiBlockFile(String testFile) throws IOException {
    OutputStream outputStream = mUfs.create(testFile);
    // Write multiple blocks of data
    int numBlocks = 3;
    // Test block size is small enough for 'int'
    int blockSize = (int) mConfiguration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    int numCopies = numBlocks * blockSize / TEST_BYTES.length;
    for (int i = 0; i < numCopies; ++i) {
      outputStream.write(TEST_BYTES);
    }
    outputStream.close();
    return numCopies;
  }

  /**
   * Prepare an object store for testing by creating a set of files and directories directly (not
   * through Alluxio). No breadcrumbs are created for directories.
   *
   * @return configuration for the pre-populated objects
   */
  private ObjectStorePreConfig prepareObjectStore() throws IOException {
    // Base directory for list status
    String baseDirectoryName = "base";
    String baseDirectoryPath = PathUtils.concatPath(mTopLevelTestDirectory, baseDirectoryName);
    String baseDirectoryKey =
        baseDirectoryPath.substring(PathUtils.normalizePath(mUfsPath, "/").length());
    // Pseudo-directories to be inferred
    String[] subDirectories = {"a", "b", "c"};
    // Every directory (base and pseudo) has these files
    String[] childrenFiles = {"sample1.jpg", "sample2.jpg", "sample3.jpg"};
    // Populate children of base directory
    for (String child : childrenFiles) {
      mUfs.create(String.format("%s/%s", baseDirectoryKey, child)).close();
    }
    // Populate children of sub-directories
    for (String subdir : subDirectories) {
      for (String child : childrenFiles) {
        mUfs.create(String.format("%s/%s/%s", baseDirectoryKey, subdir, child)).close();
      }
    }
    return new ObjectStorePreConfig(baseDirectoryPath, childrenFiles, subDirectories);
  }

  /**
   * Test configuration for pre-populating an object store.
   */
  private class ObjectStorePreConfig {
    private String mBaseDirectoryPath;
    private String[] mSubDirectoryNames;
    private String[] mFileNames;

    /**
     * Constructs {@link ObjectStorePreConfig} for pre-population an object store.
     *
     * @param baseDirectoryKey the base directory key for pre-populating of an object store
     * @param childrenFiles the children files for pre-populating an object store
     * @param subDirectories the sub-directories for pre-populating an object store
     */
    ObjectStorePreConfig(String baseDirectoryKey, String[] childrenFiles, String[] subDirectories) {
      mBaseDirectoryPath = baseDirectoryKey;
      mFileNames = childrenFiles;
      mSubDirectoryNames = subDirectories;
    }

    /**
     * @return base directory path for pre-populating an object store
     */
    String getBaseDirectoryPath() {
      return mBaseDirectoryPath;
    }

    /**
     * @return filenames for pre-populating an object store
     */
    String[] getFileNames() {
      return mFileNames;
    }

    /**
     * @return names of sub-directories for pre-populating an object store
     */
    String[] getSubDirectoryNames() {
      return mSubDirectoryNames;
    }
  }
}
