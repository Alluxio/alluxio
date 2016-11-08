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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.LoadMetadataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public final class UnderStorageSystemInterfaceIntegrationTest {
  private static final byte[] TEST_BYTES = "TestBytes".getBytes();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private String mUnderfsAddress = null;
  private UnderFileSystem mUfs = null;

  @Before
  public final void before() throws Exception {
    Configuration.set(PropertyKey.UNDERFS_LISTING_LENGTH, 50);
    mUnderfsAddress = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(mUnderfsAddress + AlluxioURI.SEPARATOR);
  }

  @After
  public final void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests that an empty file can be created.
   */
  @Test
  public void createEmpty() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.exists(testFile));
  }

  /**
   * Tests that a file can be created and validates the data written to it.
   */
  @Test
  public void createOpen() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createTestBytesFile(testFile);
    byte[] buf = new byte[TEST_BYTES.length];
    int bytesRead = mUfs.open(testFile).read(buf);
    Assert.assertTrue(bytesRead == TEST_BYTES.length);
    Assert.assertTrue(Arrays.equals(buf, TEST_BYTES));
  }

  /**
   * Tests a file can be deleted.
   */
  @Test
  public void deleteFile() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    mUfs.delete(testFile, false);
    Assert.assertFalse(mUfs.exists(testFile));
  }

  /**
   * Tests an empty directory can be deleted.
   * Tests a non empty directory will not be deleted if recursive is not specified.
   * Tests a non empty directory will be deleted if recursive is specified.
   */
  @Test
  public void deleteDir() throws IOException {
    String testDirEmpty = PathUtils.concatPath(mUnderfsAddress, "testDirEmpty");
    String testDirNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testDirNonEmpty1");
    String testDirNonEmptyChildDir = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmpty2");
    String testDirNonEmptyChildFile = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmptyF");
    String testDirNonEmptyChildDirFile = PathUtils.concatPath(testDirNonEmptyChildDir,
        "testDirNonEmptyChildDirF");
    mUfs.mkdirs(testDirEmpty, false);
    mUfs.mkdirs(testDirNonEmpty, false);
    mUfs.mkdirs(testDirNonEmptyChildDir, false);
    createEmptyFile(testDirNonEmptyChildFile);
    createEmptyFile(testDirNonEmptyChildDirFile);
    mUfs.delete(testDirEmpty, false);
    Assert.assertFalse(mUfs.exists(testDirEmpty));
    try {
      mUfs.delete(testDirNonEmpty, false);
    } catch (IOException e) {
      // Some File systems may throw IOException
    }
    Assert.assertTrue(mUfs.exists(testDirNonEmpty));
    mUfs.delete(testDirNonEmpty, true);
    Assert.assertFalse(mUfs.exists(testDirNonEmpty));
    Assert.assertFalse(mUfs.exists(testDirNonEmptyChildDir));
    Assert.assertFalse(mUfs.exists(testDirNonEmptyChildFile));
    Assert.assertFalse(mUfs.exists(testDirNonEmptyChildDirFile));
  }

  /**
   * Tests if delete deletes all files or folders for a large directory.
   */
  @Test
  public void deleteLargeDirectory() throws IOException {
    LargeDirectoryConfig config = prepareLargeDirectoryTest();
    mUfs.delete(config.getTopLevelDirectory(), true);

    String[] children = config.getChildren();
    for (String child : children) {
      // Retry for some time to allow list operation eventual consistency for S3 and GCS.
      // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
      // https://cloud.google.com/storage/docs/consistency for more details.
      // Note: not using CommonUtils.waitFor here because we intend to sleep with a longer interval.
      boolean childDeleted = false;
      for (int i = 0; i < 20; i++) {
        childDeleted = !mUfs.exists(child);
        if (childDeleted) {
          break;
        }
        CommonUtils.sleepMs(500);
      }
      Assert.assertTrue(childDeleted);
    }
  }

  /**
   * Tests exists correctly returns true if the file exists and false if it does not.
   * Tests exists correctly returns true if the dir exists and false if it does not.
   */
  @Test
  public void exists() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    Assert.assertFalse(mUfs.exists(testFile));
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.exists(testFile));
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    Assert.assertFalse(mUfs.exists(testDir));
    mUfs.mkdirs(testDir, false);
    Assert.assertTrue(mUfs.exists(testDir));
  }

  /**
   * Tests {@link UnderFileSystem#getFileSize(String)} correctly returns the file size.
   */
  @Test
  public void getFileSize() throws IOException {
    String testFileEmpty = PathUtils.concatPath(mUnderfsAddress, "testFileEmpty");
    String testFileNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testFileNonEmpty");
    createEmptyFile(testFileEmpty);
    createTestBytesFile(testFileNonEmpty);
    Assert.assertEquals(mUfs.getFileSize(testFileEmpty), 0);
    Assert.assertEquals(mUfs.getFileSize(testFileNonEmpty), TEST_BYTES.length);
  }

  /**
   * Tests {@link UnderFileSystem#getModificationTimeMs(String)} returns a reasonably accurate time.
   */
  @Test
  public void getModTime() throws IOException {
    long slack = 1000; // Some file systems may report nearest second.
    long start = System.currentTimeMillis();
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createTestBytesFile(testFile);
    long end = System.currentTimeMillis();
    long modTime = mUfs.getModificationTimeMs(testFile);
    Assert.assertTrue(modTime >= start - slack);
    Assert.assertTrue(modTime <= end + slack);
  }

  /**
   * Tests if {@link UnderFileSystem#isFile(String)} correctly returns true for files and false
   * otherwise.
   */
  @Test
  public void isFile() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    Assert.assertFalse(mUfs.isFile(testFile));
    createEmptyFile(testFile);
    mUfs.mkdirs(testDir, false);
    Assert.assertTrue(mUfs.isFile(testFile));
    Assert.assertFalse(mUfs.isFile(testDir));
  }

  /**
   * Tests if list correctly returns file names.
   */
  @Test
  public void list() throws IOException {
    String testDirNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testDirNonEmpty1");
    String testDirNonEmptyChildDir = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmpty2");
    String testDirNonEmptyChildFile = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmptyF");
    String testDirNonEmptyChildDirFile =
        PathUtils.concatPath(testDirNonEmptyChildDir, "testDirNonEmptyChildDirF");
    mUfs.mkdirs(testDirNonEmpty, false);
    mUfs.mkdirs(testDirNonEmptyChildDir, false);
    createEmptyFile(testDirNonEmptyChildFile);
    createEmptyFile(testDirNonEmptyChildDirFile);
    String [] expectedResTopDir = new String[] {"testDirNonEmpty2", "testDirNonEmptyF"};
    // Some file systems may prefix with a slash
    String [] expectedResTopDir2 = new String[] {"/testDirNonEmpty2", "/testDirNonEmptyF"};
    Arrays.sort(expectedResTopDir);
    Arrays.sort(expectedResTopDir2);
    String [] resTopDir = mUfs.list(testDirNonEmpty);
    Arrays.sort(resTopDir);
    Assert.assertTrue(Arrays.equals(expectedResTopDir, resTopDir)
        || Arrays.equals(expectedResTopDir2, resTopDir));
    Assert.assertTrue(mUfs.list(testDirNonEmptyChildDir)[0].equals("testDirNonEmptyChildDirF")
        || mUfs.list(testDirNonEmptyChildDir)[0].equals("/testDirNonEmptyChildDirF"));
  }

  /**
   * Tests if list correctly returns file or folder names for a large directory.
   */
  @Test
  public void listLargeDirectory() throws IOException {
    LargeDirectoryConfig config = prepareLargeDirectoryTest();
    String[] children = config.getChildren();

    // Retry for some time to allow list operation eventual consistency for S3 and GCS.
    // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
    // https://cloud.google.com/storage/docs/consistency for more details.
    // Note: not using CommonUtils.waitFor here because we intend to sleep with a longer interval.
    String[] results = new String[] {};
    for (int i = 0; i < 20; i++) {
      results = mUfs.list(config.getTopLevelDirectory());
      if (children.length == results.length) {
        break;
      }
      CommonUtils.sleepMs(500);
    }
    Assert.assertEquals(children.length, results.length);

    Arrays.sort(results);
    for (int i = 0; i < children.length; ++i) {
      Assert.assertTrue(results[i].equals(CommonUtils.stripPrefixIfPresent(children[i],
          PathUtils.normalizePath(config.getTopLevelDirectory(), "/"))));
    }
  }

  /**
   * Tests if list recursive correctly returns all file names in all subdirectories.
   */
  @Test
  public void listRecursive() throws IOException {
    String root = mUnderfsAddress;
    // TODO(andrew): Should this directory be created in LocalAlluxioCluster creation code?
    mUfs.mkdirs(root, true);
    // Empty lsr should be empty
    Assert.assertEquals(0, mUfs.listRecursive(root).length);

    // Create a tree of subdirectories and files
    String sub1 = PathUtils.concatPath(root, "sub1");
    String sub2 = PathUtils.concatPath(root, "sub2");
    String sub11 = PathUtils.concatPath(sub1, "sub11");
    String file11 = PathUtils.concatPath(sub11, "file11");
    String file2 = PathUtils.concatPath(sub2, "file2");
    String file = PathUtils.concatPath(root, "file");
    // lsr of nonexistent path should be null
    Assert.assertNull(mUfs.listRecursive(sub1));

    mUfs.mkdirs(sub1, false);
    mUfs.mkdirs(sub2, false);
    mUfs.mkdirs(sub11, false);
    createEmptyFile(file11);
    createEmptyFile(file2);
    createEmptyFile(file);

    // lsr from root should return paths relative to the root
    String[] expectedResRoot =
        {"sub1", "sub2", "sub1/sub11", "sub1/sub11/file11", "sub2/file2", "file"};
    String[] actualResRoot = mUfs.listRecursive(root);
    Arrays.sort(expectedResRoot);
    Arrays.sort(actualResRoot);
    Assert.assertArrayEquals(expectedResRoot, actualResRoot);

    // lsr from sub1 should return paths relative to sub1
    String[] expectedResSub1 = {"sub11", "sub11/file11"};
    String[] actualResSub1 = mUfs.listRecursive(sub1);
    Arrays.sort(expectedResSub1);
    Arrays.sort(actualResSub1);
    Assert.assertArrayEquals(expectedResSub1, actualResSub1);

    // lsr of file should be null
    Assert.assertNull(mUfs.listRecursive(file));
  }

  /**
   * Tests {@link UnderFileSystem#mkdirs(String, boolean)} correctly creates a directory.
   * Tests {@link UnderFileSystem#mkdirs(String, boolean)} correctly makes parent directories if
   * createParent is specified.
   */
  @Test
  public void mkdirs() throws IOException {
    // make sure the underfs address dir exists already
    mUfs.mkdirs(mUnderfsAddress, true);
    // empty lsr should be empty
    Assert.assertEquals(0, mUfs.listRecursive(mUnderfsAddress).length);

    String testDirTop = PathUtils.concatPath(mUnderfsAddress, "testDirTop");
    String testDir1 = PathUtils.concatPath(mUnderfsAddress, "1");
    String testDir2 = PathUtils.concatPath(testDir1, "2");
    String testDir3 = PathUtils.concatPath(testDir2, "3");
    String testDirDeep = PathUtils.concatPath(testDir3, "testDirDeep");
    mUfs.mkdirs(testDirTop, false);
    Assert.assertTrue(mUfs.exists(testDirTop));
    mUfs.mkdirs(testDirDeep, true);
    Assert.assertTrue(mUfs.exists(testDir1));
    Assert.assertTrue(mUfs.exists(testDir2));
    Assert.assertTrue(mUfs.exists(testDir3));
    Assert.assertTrue(mUfs.exists(testDirDeep));
  }

  /**
   * Tests {@link UnderFileSystem#rename(String, String)} works file to new location.
   */
  @Test
  public void renameFile() throws IOException {
    String testFileSrc = PathUtils.concatPath(mUnderfsAddress, "testFileSrc");
    String testFileDst = PathUtils.concatPath(mUnderfsAddress, "testFileDst");
    createEmptyFile(testFileSrc);
    mUfs.rename(testFileSrc, testFileDst);
    Assert.assertFalse(mUfs.exists(testFileSrc));
    Assert.assertTrue(mUfs.exists(testFileDst));
  }

  /**
   * Tests {@link UnderFileSystem#rename(String, String)} works file to a folder if supported.
   */
  @Test
  public void renameFileToFolder() throws IOException {
    String testFileSrc = PathUtils.concatPath(mUnderfsAddress, "testFileSrc");
    String testFileDst = PathUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testFileFinalDst = PathUtils.concatPath(testFileDst, "testFileSrc");
    createEmptyFile(testFileSrc);
    mUfs.mkdirs(testFileDst, false);
    if (mUfs.rename(testFileSrc, testFileDst)) {
      Assert.assertFalse(mUfs.exists(testFileSrc));
      Assert.assertTrue(mUfs.exists(testFileFinalDst));
    }
  }

  /**
   * Tests {@link UnderFileSystem#rename(String, String)} works folder to new location.
   */
  @Test
  public void renameFolder() throws IOException {
    String testDirSrc = PathUtils.concatPath(mUnderfsAddress, "testDirSrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirDst = PathUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    mUfs.mkdirs(testDirSrc, false);
    createEmptyFile(testDirSrcChild);
    mUfs.rename(testDirSrc, testDirDst);
    Assert.assertFalse(mUfs.exists(testDirSrc));
    Assert.assertFalse(mUfs.exists(testDirSrcChild));
    Assert.assertTrue(mUfs.exists(testDirDst));
    Assert.assertTrue(mUfs.exists(testDirDstChild));
  }

  /**
   * Tests {@link UnderFileSystem#rename(String, String)} works folder to another folder if
   * supported.
   */
  @Test
  public void renameFolderToFolder() throws IOException {
    String testDirSrc = PathUtils.concatPath(mUnderfsAddress, "testDirSrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirDst = PathUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    String testDirFinalDst = PathUtils.concatPath(testDirDst, "testDirSrc");
    String testDirChildFinalDst = PathUtils.concatPath(testDirFinalDst, "testFile");
    mUfs.mkdirs(testDirSrc, false);
    mUfs.mkdirs(testDirDst, false);
    createEmptyFile(testDirDstChild);
    createEmptyFile(testDirSrcChild);
    if (mUfs.rename(testDirSrc, testDirDst)) {
      Assert.assertFalse(mUfs.exists(testDirSrc));
      Assert.assertFalse(mUfs.exists(testDirSrcChild));
      Assert.assertTrue(mUfs.exists(testDirDst));
      Assert.assertTrue(mUfs.exists(testDirDstChild));
      Assert.assertTrue(mUfs.exists(testDirFinalDst));
      Assert.assertTrue(mUfs.exists(testDirChildFinalDst));
    }
  }

  /**
   * Tests load metadata on list.
   */
  @Test
  public void loadMetadata() throws Exception {
    String dirName = "loadMetaDataRoot";

    String rootDir = PathUtils.concatPath(mUnderfsAddress, dirName);
    mUfs.mkdirs(rootDir, true);

    String rootFile1 = PathUtils.concatPath(rootDir, "file1");
    createEmptyFile(rootFile1);

    String rootFile2 = PathUtils.concatPath(rootDir, "file2");
    createEmptyFile(rootFile2);

    AlluxioURI rootAlluxioURI = new AlluxioURI("/" + dirName);
    FileSystem client = mLocalAlluxioClusterResource.get().getClient();
    client.listStatus(rootAlluxioURI,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));

    try {
      client.createDirectory(rootAlluxioURI, CreateDirectoryOptions.defaults());
      Assert.fail("create is expected to fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(rootAlluxioURI), e.getMessage());
    }

    AlluxioURI file1URI = rootAlluxioURI.join("file1");
    try {
      client.createFile(file1URI, CreateFileOptions.defaults()).close();
      Assert.fail("create is expected to fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(file1URI), e.getMessage());
    }

    AlluxioURI file2URI = rootAlluxioURI.join("file2");
    try {
      client.createFile(file2URI, CreateFileOptions.defaults()).close();
      Assert.fail("create is expected to fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(file2URI), e.getMessage());
    }
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
  private LargeDirectoryConfig prepareLargeDirectoryTest() throws IOException {
    final String filePrefix = "a_";
    final String folderPrefix = "b_";

    String topLevelDirectory = PathUtils.concatPath(mUnderfsAddress, "topLevelDir");

    final int numFiles = 100;

    String[] children = new String[numFiles + numFiles];

    // Make top level directory
    mUfs.mkdirs(topLevelDirectory, false);

    // Make the children files
    for (int i = 0; i < numFiles; ++i) {
      children[i] = PathUtils.concatPath(topLevelDirectory, filePrefix
          + String.format("%04d", i));
      createEmptyFile(children[i]);
    }
    // Make the children folders
    for (int i = 0; i < numFiles; ++i) {
      children[numFiles + i] = PathUtils.concatPath(topLevelDirectory, folderPrefix
          + String.format("%04d", i));
      mUfs.mkdirs(children[numFiles + i], false);
    }

    return new LargeDirectoryConfig(topLevelDirectory, children);
  }

  // Test configuration for pagination tests
  private class LargeDirectoryConfig {
    private String mTopLevelDirectory;
    // Children for top level directory
    private String[] mChildren;

    LargeDirectoryConfig(String topLevelDirectory, String[] children) {
      mTopLevelDirectory = topLevelDirectory;
      mChildren = children;
    }

    public String getTopLevelDirectory() {
      return mTopLevelDirectory;
    }

    public String[] getChildren() {
      return mChildren;
    }
  }
}
