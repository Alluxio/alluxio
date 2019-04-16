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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;

import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

/**
 * This is the base class of UFS contract tests. It describes the contract of Alluxio with the UFS
 * through the UFS interface. Each implementation of {@link UnderFileSystem} is expected to create
 * a test that extends this base class.
 */
public abstract class AbstractUnderFileSystemContractTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractUnderFileSystemContractTest.class);

  private static final byte[] TEST_BYTES = "TestBytes".getBytes();

  private String mUnderfsAddress;
  private UnderFileSystem mUfs;
  protected InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Rule
  public final ConfigurationRule mConfRule =
      new ConfigurationRule(ImmutableMap.of(PropertyKey.UNDERFS_LISTING_LENGTH, "50",
          PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "512B",
          // Increase the buffer time of journal writes to speed up tests
          PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "1sec"), mConfiguration);

  /**
   * @param path path of UFS
   * @param conf UFS configuration
   * @return the UFS instance for test
   */
  public abstract UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception;

  /**
   * @return the UFS address for test
   */
  public abstract String getUfsBaseDir();

  @Before
  public final void before() throws Exception {
    mUnderfsAddress = PathUtils.concatPath(getUfsBaseDir(), UUID.randomUUID());
    mUfs = createUfs(mUnderfsAddress, UnderFileSystemConfiguration.defaults());
    mUfs.mkdirs(mUnderfsAddress, MkdirsOptions.defaults(mConfiguration).setCreateParent(true));
  }

  @After
  public final void after() throws Exception {
    mConfiguration = ConfigurationTestUtils.defaults();
    mUfs.deleteDirectory(mUnderfsAddress, DeleteOptions.defaults().setRecursive(true));
    mUfs.close();
  }

  @Test
  public void createAtomic() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createAtomic");
    OutputStream stream = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
        .setEnsureAtomic(true));
    stream.write(TEST_BYTES);
    assertFalse(mUfs.isFile(testFile));
    stream.close();
    assertTrue(mUfs.isFile(testFile));
  }

  @Test
  public void createEmpty() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createEmpty");
    createEmptyFile(testFile);
    assertTrue(mUfs.isFile(testFile));
  }

  @Test
  public void createNoParent() throws IOException {
    // Run the test only for local UFS. Other UFSs succeed if no parents are present
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(mUfs));

    mThrown.expect(IOException.class);
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createNoParent/testFile");
    OutputStream o = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
        .setCreateParent(false));
    o.close();
  }

  @Test
  public void createParent() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createParent/testFile");
    OutputStream o = mUfs.create(testFile, CreateOptions.defaults(mConfiguration)
        .setCreateParent(true));
    o.close();
    assertTrue(mUfs.exists(testFile));
  }

  @Test
  public void createOpen() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createOpen");
    createTestBytesFile(testFile);
    byte[] buf = new byte[TEST_BYTES.length];
    int bytesRead = mUfs.open(testFile).read(buf);
    assertEquals(TEST_BYTES.length, bytesRead);
    assertTrue(Arrays.equals(buf, TEST_BYTES));
  }

  @Test
  public void createOpenEmpty() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createOpenEmpty");
    createEmptyFile(testFile);
    byte[] buf = new byte[0];
    int bytesRead = mUfs.open(testFile).read(buf);
    // TODO(adit): Consider making the return value uniform across UFSs
    if (UnderFileSystemUtils.isHdfs(mUfs)) {
      assertEquals(-1, bytesRead);
    } else {
      assertEquals(0, bytesRead);
    }
  }

  @Test
  public void createOpenAtPosition() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createOpenAtPosition");
    prepareMultiBlockFile(testFile);
    int[] offsets = {0, 256, 511, 512, 513, 768, 1024, 1025};
    for (int offset : offsets) {
      InputStream inputStream = mUfs.open(testFile, OpenOptions.defaults().setOffset(offset));
      assertEquals(TEST_BYTES[offset % TEST_BYTES.length], inputStream.read());
      inputStream.close();
    }
  }

  @Test
  public void createOpenLarge() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "createOpenLarge");
    int numCopies = prepareMultiBlockFile(testFile);
    InputStream inputStream = mUfs.open(testFile);
    byte[] buf = new byte[numCopies * TEST_BYTES.length];
    int offset = 0;
    int noReadCount = 0;
    while (offset < buf.length && noReadCount < 3) {
      int bytesRead = inputStream.read(buf, offset, buf.length - offset);
      if (bytesRead != -1) {
        noReadCount = 0;
        for (int i = 0; i < bytesRead; ++i) {
          assertEquals(TEST_BYTES[(offset + i) % TEST_BYTES.length], buf[offset + i]);
        }
        offset += bytesRead;
      } else {
        ++noReadCount;
      }
    }
    assertTrue(noReadCount < 3);
  }

  @Test
  public void deleteFile() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "deleteFile");
    createEmptyFile(testFile);
    mUfs.deleteFile(testFile);
    assertFalse(mUfs.exists(testFile));
    assertFalse(mUfs.isFile(testFile));
  }

  @Test
  public void deleteDir() throws IOException {
    String testDirEmpty = PathUtils.concatPath(mUnderfsAddress, "testDirEmpty");
    String testDirNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testDirNonEmpty1");
    String testDirNonEmptyChildDir = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmpty2");
    String testDirNonEmptyChildFile = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmptyF");
    String testDirNonEmptyChildDirFile =
        PathUtils.concatPath(testDirNonEmptyChildDir, "testDirNonEmptyChildDirF");
    mUfs.mkdirs(testDirEmpty, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(testDirNonEmpty, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    mUfs.mkdirs(testDirNonEmptyChildDir, MkdirsOptions.defaults(mConfiguration)
        .setCreateParent(false));
    createEmptyFile(testDirNonEmptyChildFile);
    createEmptyFile(testDirNonEmptyChildDirFile);
    mUfs.deleteDirectory(testDirEmpty, DeleteOptions.defaults().setRecursive(false));
    assertFalse(mUfs.isDirectory(testDirEmpty));
    try {
      mUfs.deleteDirectory(testDirNonEmpty, DeleteOptions.defaults().setRecursive(false));
    } catch (IOException e) {
      // Some File systems may throw IOException
    }
    assertTrue(mUfs.isDirectory(testDirNonEmpty));
    mUfs.deleteDirectory(testDirNonEmpty, DeleteOptions.defaults().setRecursive(true));
    assertFalse(mUfs.isDirectory(testDirNonEmpty));
    assertFalse(mUfs.isDirectory(testDirNonEmptyChildDir));
    assertFalse(mUfs.isFile(testDirNonEmptyChildFile));
    assertFalse(mUfs.isFile(testDirNonEmptyChildDirFile));
  }

  @Test
  public void deleteLargeDirectory() throws IOException {
    LargeDirectoryConfig config = prepareLargeDirectoryTest();
    mUfs.deleteDirectory(config.getTopLevelDirectory(),
        DeleteOptions.defaults().setRecursive(true));

    String[] children = config.getChildren();
    for (String child : children) {
      // Retry for some time to allow list operation eventual consistency for S3 and GCS.
      // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
      // https://cloud.google.com/storage/docs/consistency for more details.
      // Note: not using CommonUtils.waitFor here because we intend to sleep with a longer interval.
      boolean childDeleted = false;
      for (int i = 0; i < 20; i++) {
        childDeleted = !mUfs.isFile(child) && !mUfs.isDirectory(child);
        if (childDeleted) {
          break;
        }
        CommonUtils.sleepMs(500);
      }
      assertTrue(childDeleted);
    }
  }

  @Test
  public void createDeleteFileConjuctionTest() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "deleteThenCreateNonexistingFile");
    createTestBytesFile(testFile);
    assertTrue(mUfs.exists(testFile));
    assertTrue(mUfs.isFile(testFile));

    mUfs.deleteExistingFile(testFile);
    assertFalse(mUfs.exists(testFile));

    OutputStream o = mUfs.createNonexistingFile(testFile);
    o.write(TEST_BYTES);
    o.close();
    assertTrue(mUfs.exists(testFile));
  }

  @Test
  public void createThenDeleteExistingDirectoryTest() throws IOException {
    LargeDirectoryConfig config = prepareLargeDirectoryTest();
    mUfs.deleteExistingDirectory(config.getTopLevelDirectory(),
        DeleteOptions.defaults().setRecursive(true));

    for (String child : config.getChildren()) {
      assertTrue(!mUfs.isFile(child) && !mUfs.isDirectory(child));
    }
  }

  @Test
  public void exists() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    assertFalse(mUfs.isFile(testFile));
    createEmptyFile(testFile);
    assertTrue(mUfs.isFile(testFile));
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    assertFalse(mUfs.isDirectory(testDir));
    mUfs.mkdirs(testDir, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    assertTrue(mUfs.isDirectory(testDir));
  }

  @Test
  public void getDirectoryStatus() throws IOException {
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    mUfs.mkdirs(testDir);

    UfsStatus status = mUfs.getStatus(testDir);
    assertTrue(status instanceof UfsDirectoryStatus);
  }

  @Test
  public void createThenGetExistingDirectoryStatus() throws IOException {
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    mUfs.mkdirs(testDir);
    UfsStatus status = mUfs.getExistingStatus(testDir);
    assertTrue(status instanceof UfsDirectoryStatus);
  }

  @Test
  public void getFileSize() throws IOException {
    String testFileEmpty = PathUtils.concatPath(mUnderfsAddress, "testFileEmpty");
    String testFileNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testFileNonEmpty");
    createEmptyFile(testFileEmpty);
    createTestBytesFile(testFileNonEmpty);
    assertEquals(mUfs.getFileStatus(testFileEmpty).getContentLength(), 0);
    assertEquals(mUfs.getFileStatus(testFileNonEmpty).getContentLength(), TEST_BYTES.length);
  }

  @Test
  public void createThenGetExistingFileStatus() throws IOException {
    String testFileNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testFileNonEmpty");
    String testFileLarge = PathUtils.concatPath(mUnderfsAddress, "testFileLarge");
    createTestBytesFile(testFileNonEmpty);
    int numCopies = prepareMultiBlockFile(testFileLarge);
    assertEquals(TEST_BYTES.length,
        mUfs.getExistingFileStatus(testFileNonEmpty).getContentLength());
    assertEquals(TEST_BYTES.length * numCopies,
        mUfs.getExistingFileStatus(testFileLarge).getContentLength());
  }

  @Test
  public void getFileStatus() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);

    UfsStatus status = mUfs.getStatus(testFile);
    assertTrue(status instanceof UfsFileStatus);
  }

  @Test
  public void createThenGetExistingStatus() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createTestBytesFile(testFile);

    UfsStatus status = mUfs.getExistingStatus(testFile);
    assertTrue(status instanceof UfsFileStatus);
  }

  @Test
  public void getModTime() throws IOException {
    long slack = 5000; // Some file systems may report nearest second.
    long start = System.currentTimeMillis();
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createTestBytesFile(testFile);
    long end = System.currentTimeMillis();
    long modTime = mUfs.getFileStatus(testFile).getLastModifiedTime();
    assertTrue(modTime >= start - slack);
    assertTrue(modTime <= end + slack);
  }

  @Test
  public void isFile() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    assertFalse(mUfs.isFile(testFile));
    createEmptyFile(testFile);
    mUfs.mkdirs(testDir, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    assertTrue(mUfs.isFile(testFile));
    assertFalse(mUfs.isFile(testDir));
  }

  @Test
  public void listStatus() throws IOException {
    String testDirNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testDirNonEmpty1");
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
    assertTrue(Arrays.equals(expectedResTopDir, resTopDir)
        || Arrays.equals(expectedResTopDir2, resTopDir));
    assertTrue(
        mUfs.listStatus(testDirNonEmptyChildDir)[0].getName().equals("testDirNonEmptyChildDirF")
            || mUfs.listStatus(testDirNonEmptyChildDir)[0].getName()
                .equals("/testDirNonEmptyChildDirF"));
    for (int i = 0; i < resTopDir.length; ++i) {
      assertEquals(
          mUfs.isDirectory(PathUtils.concatPath(testDirNonEmpty, resTopDirStatus[i].getName())),
          resTopDirStatus[i].isDirectory());
    }
  }

  @Test
  public void listStatusEmpty() throws IOException {
    String testDir = PathUtils.concatPath(mUnderfsAddress, "listStatusEmpty");
    mUfs.mkdirs(testDir);
    UfsStatus[] res = mUfs.listStatus(testDir);
    assertEquals(0, res.length);
  }

  @Test
  public void listStatusFile() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "listStatusFile");
    createEmptyFile(testFile);
    assertTrue(mUfs.listStatus(testFile) == null);
  }

  @Test
  public void listLargeDirectory() throws IOException {
    LargeDirectoryConfig config = prepareLargeDirectoryTest();
    String[] children = config.getChildren();

    // Retry for some time to allow list operation eventual consistency for S3 and GCS.
    // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
    // https://cloud.google.com/storage/docs/consistency for more details.
    // Note: not using CommonUtils.waitFor here because we intend to sleep with a longer interval.
    UfsStatus[] results = new UfsStatus[] {};
    for (int i = 0; i < 20; i++) {
      results = mUfs.listStatus(config.getTopLevelDirectory());
      if (children.length == results.length) {
        break;
      }
      CommonUtils.sleepMs(500);
    }
    assertEquals(children.length, results.length);

    String[] resultNames = UfsStatus.convertToNames(results);
    Arrays.sort(resultNames);
    for (int i = 0; i < children.length; ++i) {
      assertTrue(resultNames[i].equals(CommonUtils.stripPrefixIfPresent(children[i],
          PathUtils.normalizePath(config.getTopLevelDirectory(), "/"))));
    }
  }

  @Test
  public void listStatusRecursive() throws IOException {
    String root = mUnderfsAddress;
    // TODO(andrew): Should this directory be created in LocalAlluxioCluster creation code?
    mUfs.mkdirs(root);
    // Empty lsr should be empty
    assertEquals(0, mUfs.listStatus(root).length);

    // Create a tree of subdirectories and files
    String sub1 = PathUtils.concatPath(root, "sub1");
    String sub2 = PathUtils.concatPath(root, "sub2");
    String sub11 = PathUtils.concatPath(sub1, "sub11");
    String file11 = PathUtils.concatPath(sub11, "file11");
    String file2 = PathUtils.concatPath(sub2, "file2");
    String file = PathUtils.concatPath(root, "file");
    // lsr of nonexistent path should be null
    assertNull(mUfs.listStatus(sub1, ListOptions.defaults().setRecursive(true)));

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
    assertArrayEquals(expectedResRoot, actualResRoot);
    for (int i = 0; i < actualResRoot.length; ++i) {
      assertEquals(
          mUfs.isDirectory(PathUtils.concatPath(root, actualResRootStatus[i].getName())),
          actualResRootStatus[i].isDirectory());
    }
    // lsr from sub1 should return paths relative to sub1
    String[] expectedResSub1 = {"sub11", "sub11/file11"};
    String[] actualResSub1 =
        UfsStatus.convertToNames(mUfs.listStatus(sub1, ListOptions.defaults().setRecursive(true)));
    Arrays.sort(expectedResSub1);
    Arrays.sort(actualResSub1);
    assertArrayEquals(expectedResSub1, actualResSub1);

    // lsr of file should be null
    assertNull(mUfs.listStatus(file, ListOptions.defaults().setRecursive(true)));
  }

  @Test
  public void mkdirs() throws IOException {
    // make sure the underfs address dir exists already
    mUfs.mkdirs(mUnderfsAddress);
    // empty lsr should be empty
    assertEquals(0, mUfs.listStatus(mUnderfsAddress).length);

    String testDirTop = PathUtils.concatPath(mUnderfsAddress, "testDirTop");
    String testDir1 = PathUtils.concatPath(mUnderfsAddress, "1");
    String testDir2 = PathUtils.concatPath(testDir1, "2");
    String testDir3 = PathUtils.concatPath(testDir2, "3");
    String testDirDeep = PathUtils.concatPath(testDir3, "testDirDeep");
    mUfs.mkdirs(testDirTop, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    assertTrue(mUfs.isDirectory(testDirTop));
    mUfs.mkdirs(testDirDeep, MkdirsOptions.defaults(mConfiguration).setCreateParent(true));
    assertTrue(mUfs.isDirectory(testDir1));
    assertTrue(mUfs.isDirectory(testDir2));
    assertTrue(mUfs.isDirectory(testDir3));
    assertTrue(mUfs.isDirectory(testDirDeep));
  }

  @Test
  public void objectCommonPrefixesIsDirectory() throws IOException {
    // Only run test for an object store
    Assume.assumeTrue(mUfs.isObjectStorage());

    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs;
    ObjectStorePreConfig config = prepareObjectStore(ufs);

    String baseDirectoryPath = config.getBaseDirectoryPath();
    assertTrue(mUfs.isDirectory(baseDirectoryPath));

    for (String subDirName : config.getSubDirectoryNames()) {
      String subDirPath = PathUtils.concatPath(baseDirectoryPath, subDirName);
      assertTrue(mUfs.isDirectory(subDirPath));
    }
  }

  @Test
  public void objectCommonPrefixesListStatusNonRecursive() throws IOException {
    // Only run test for an object store
    Assume.assumeTrue(mUfs.isObjectStorage());

    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs;
    ObjectStorePreConfig config = prepareObjectStore(ufs);

    String baseDirectoryPath = config.getBaseDirectoryPath();
    UfsStatus[] results = mUfs.listStatus(baseDirectoryPath);
    assertEquals(config.getSubDirectoryNames().length + config.getFileNames().length,
        results.length);
    // Check for direct children files
    for (String fileName : config.getFileNames()) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(fileName)) {
          foundIndex = i;
        }
      }
      assertTrue(foundIndex >= 0);
      assertTrue(results[foundIndex].isFile());
    }
    // Check if pseudo-directories were inferred
    for (String subDirName : config.getSubDirectoryNames()) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(subDirName)) {
          foundIndex = i;
        }
      }
      assertTrue(foundIndex >= 0);
      assertTrue(results[foundIndex].isDirectory());
    }
  }

  @Test
  public void objectCommonPrefixesListStatusRecursive() throws IOException {
    // Only run test for an object store
    Assume.assumeTrue(mUfs.isObjectStorage());

    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs;
    ObjectStorePreConfig config = prepareObjectStore(ufs);

    String baseDirectoryPath = config.getBaseDirectoryPath();
    UfsStatus[] results =
        mUfs.listStatus(baseDirectoryPath, ListOptions.defaults().setRecursive(true));
    String[] fileNames = config.getFileNames();
    String[] subDirNames = config.getSubDirectoryNames();
    assertEquals(
        subDirNames.length + fileNames.length + subDirNames.length * fileNames.length,
        results.length);
    // Check for direct children files
    for (String fileName : fileNames) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(fileName)) {
          foundIndex = i;
        }
      }
      assertTrue(foundIndex >= 0);
      assertTrue(results[foundIndex].isFile());
    }
    for (String subDirName : subDirNames) {
      // Check if pseudo-directories were inferred
      int dirIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(subDirName)) {
          dirIndex = i;
        }
      }
      assertTrue(dirIndex >= 0);
      assertTrue(results[dirIndex].isDirectory());
      // Check for indirect children
      for (String fileName : config.getFileNames()) {
        int fileIndex = -1;
        for (int i = 0; i < results.length; ++i) {
          if (results[i].getName().equals(String.format("%s/%s", subDirName, fileName))) {
            fileIndex = i;
          }
        }
        assertTrue(fileIndex >= 0);
        assertTrue(results[fileIndex].isFile());
      }
    }
  }

  @Test
  public void objectNestedDirsListStatusRecursive() throws IOException {
    // Only run test for an object store
    Assume.assumeTrue(mUfs.isObjectStorage());
    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs;
    String root = mUnderfsAddress;
    int nesting = 5;

    String path = root;
    for (int i = 0; i < nesting; i++) {
      path = PathUtils.concatPath(path, "dir" + i);
    }
    String file1 = PathUtils.concatPath(path, "file.txt");

    // Empty lsr should be empty
    assertEquals(0, mUfs.listStatus(root, ListOptions.defaults().setRecursive(true)).length);

    String fileKey = file1.substring(PathUtils.normalizePath(ufs.getRootKey(), "/").length());
    assertTrue(ufs.createEmptyObject(fileKey));

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
    assertEquals(expectedStatus.length, actualStatus.length);
    Arrays.sort(expectedStatus);
    Arrays.sort(actualStatus);
    assertArrayEquals(expectedStatus, actualStatus);
  }

  @Test
  public void renameFile() throws IOException {
    String testFileSrc = PathUtils.concatPath(mUnderfsAddress, "renameFileSrc");
    String testFileDst = PathUtils.concatPath(mUnderfsAddress, "renameFileDst");
    createEmptyFile(testFileSrc);
    mUfs.renameFile(testFileSrc, testFileDst);
    assertFalse(mUfs.isFile(testFileSrc));
    assertTrue(mUfs.isFile(testFileDst));
  }

  @Test
  public void enameRenamableFile() throws IOException {
    String testFileSrc = PathUtils.concatPath(mUnderfsAddress, "renameFileSrc");
    String testFileDst = PathUtils.concatPath(mUnderfsAddress, "renameFileDst");
    prepareMultiBlockFile(testFileSrc);
    mUfs.renameRenamableFile(testFileSrc, testFileDst);
    assertFalse(mUfs.isFile(testFileSrc));
    assertTrue(mUfs.isFile(testFileDst));
  }

  @Test
  public void renameDirectory() throws IOException {
    String testDirSrc = PathUtils.concatPath(mUnderfsAddress, "renameDirectorySrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirDst = PathUtils.concatPath(mUnderfsAddress, "renameDirectoryDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    createEmptyFile(testDirSrcChild);
    mUfs.renameDirectory(testDirSrc, testDirDst);
    assertFalse(mUfs.isDirectory(testDirSrc));
    assertFalse(mUfs.isFile(testDirSrcChild));
    assertTrue(mUfs.isDirectory(testDirDst));
    assertTrue(mUfs.isFile(testDirDstChild));
  }

  @Test
  public void renameDirectoryDeep() throws IOException {
    String testDirSrc = PathUtils.concatPath(mUnderfsAddress, "renameDirectoryDeepSrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirSrcNested = PathUtils.concatPath(testDirSrc, "testNested");
    String testDirSrcNestedChild = PathUtils.concatPath(testDirSrcNested, "testNestedFile");

    String testDirDst = PathUtils.concatPath(mUnderfsAddress, "renameDirectoryDeepDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    String testDirDstNested = PathUtils.concatPath(testDirDst, "testNested");
    String testDirDstNestedChild = PathUtils.concatPath(testDirDstNested, "testNestedFile");

    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    createEmptyFile(testDirSrcChild);
    mUfs.mkdirs(testDirSrcNested, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    createEmptyFile(testDirSrcNestedChild);

    mUfs.renameDirectory(testDirSrc, testDirDst);

    assertFalse(mUfs.isDirectory(testDirSrc));
    assertFalse(mUfs.isFile(testDirSrcChild));
    assertFalse(mUfs.isDirectory(testDirSrcNested));
    assertFalse(mUfs.isFile(testDirSrcNestedChild));

    assertTrue(mUfs.isDirectory(testDirDst));
    assertTrue(mUfs.isFile(testDirDstChild));
    assertTrue(mUfs.isDirectory(testDirDstNested));
    assertTrue(mUfs.isFile(testDirDstNestedChild));
  }

  @Test
  public void renameRenameableDirectory() throws IOException {
    String testDirSrc = PathUtils.concatPath(mUnderfsAddress, "renameRenamableDirectorySrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirSrcNested = PathUtils.concatPath(testDirSrc, "testNested");
    String testDirSrcNestedChild = PathUtils.concatPath(testDirSrcNested, "testNestedFile");

    String testDirDst = PathUtils.concatPath(mUnderfsAddress, "renameRenamableDirectoryDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    String testDirDstNested = PathUtils.concatPath(testDirDst, "testNested");
    String testDirDstNestedChild = PathUtils.concatPath(testDirDstNested, "testNestedFile");

    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    prepareMultiBlockFile(testDirSrcChild);
    mUfs.mkdirs(testDirSrcNested, MkdirsOptions.defaults(mConfiguration).setCreateParent(false));
    prepareMultiBlockFile(testDirSrcNestedChild);

    mUfs.renameRenamableDirectory(testDirSrc, testDirDst);

    assertFalse(mUfs.isDirectory(testDirSrc));
    assertFalse(mUfs.isFile(testDirSrcChild));
    assertFalse(mUfs.isDirectory(testDirSrcNested));
    assertFalse(mUfs.isFile(testDirSrcNestedChild));

    assertTrue(mUfs.isDirectory(testDirDst));
    assertTrue(mUfs.isFile(testDirDstChild));
    assertTrue(mUfs.isDirectory(testDirDstNested));
    assertTrue(mUfs.isFile(testDirDstNestedChild));
  }

  @Test
  public void renameLargeDirectory() throws Exception {
    LargeDirectoryConfig config = prepareLargeDirectoryTest();
    String dstTopLevelDirectory = PathUtils.concatPath(mUnderfsAddress, "topLevelDirMoved");
    mUfs.renameDirectory(config.getTopLevelDirectory(), dstTopLevelDirectory);
    // 1. Check the src directory no longer exists
    String[] srcChildren = config.getChildren();
    for (String src : srcChildren) {
      // Retry for some time to allow list operation eventual consistency for S3 and GCS.
      // See http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html and
      // https://cloud.google.com/storage/docs/consistency for more details.
      CommonUtils.waitFor("list after delete consistency", () -> {
        try {
          return !mUfs.exists(src);
        } catch (IOException e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(10000).setInterval(500));
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
          assertTrue(resultNames[i].equals(CommonUtils.stripPrefixIfPresent(srcChildren[i],
              PathUtils.normalizePath(config.getTopLevelDirectory(), "/"))));
        }
      } catch (IOException e) {
        return false;
      }
      return true;
    }, WaitForOptions.defaults().setTimeoutMs(10000).setInterval(500));
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
    public String getTopLevelDirectory() {
      return mTopLevelDirectory;
    }

    /**
     * @return the children files of the directory tree for pagination tests
     */
    public String[] getChildren() {
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
   * @param ufs the {@link ObjectUnderFileSystem} to test
   * @return configuration for the pre-populated objects
   */
  private ObjectStorePreConfig prepareObjectStore(ObjectUnderFileSystem ufs) throws IOException {
    // Base directory for list status
    String baseDirectoryName = "base";
    String baseDirectoryPath = PathUtils.concatPath(mUnderfsAddress, baseDirectoryName);
    String baseDirectoryKey =
        baseDirectoryPath.substring(PathUtils.normalizePath(ufs.getRootKey(), "/").length());
    // Pseudo-directories to be inferred
    String[] subDirectories = {"a", "b", "c"};
    // Every directory (base and pseudo) has these files
    String[] childrenFiles = {"sample1.jpg", "sample2.jpg", "sample3.jpg"};
    // Populate children of base directory
    for (String child : childrenFiles) {
      ufs.createEmptyObject(String.format("%s/%s", baseDirectoryKey, child));
    }
    // Populate children of sub-directories
    for (String subdir : subDirectories) {
      for (String child : childrenFiles) {
        ufs.createEmptyObject(String.format("%s/%s/%s", baseDirectoryKey, subdir, child));
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
    public String getBaseDirectoryPath() {
      return mBaseDirectoryPath;
    }

    /**
     * @return filenames for pre-populating an object store
     */
    public String[] getFileNames() {
      return mFileNames;
    }

    /**
     * @return names of sub-directories for pre-populating an object store
     */
    public String[] getSubDirectoryNames() {
      return mSubDirectoryNames;
    }
  }
}
