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
import alluxio.Seekable;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.LoadMetadataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

public final class UnderStorageSystemInterfaceIntegrationTest {
  private static final byte[] TEST_BYTES = "TestBytes".getBytes();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private String mUnderfsAddress = null;
  private UnderFileSystemWithLogging mUfs = null;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() throws Exception {
    Configuration.set(PropertyKey.UNDERFS_LISTING_LENGTH, 50);
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "512B");
    mUnderfsAddress = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    mUfs = (UnderFileSystemWithLogging) UnderFileSystem.Factory.getForRoot();
  }

  @After
  public final void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests if file creation is atomic.
   */
  @Test
  public void createAtomic() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    OutputStream stream = mUfs.create(testFile);
    stream.write(TEST_BYTES);
    Assert.assertFalse(mUfs.isFile(testFile));
    stream.close();
    Assert.assertTrue(mUfs.isFile(testFile));
  }

  /**
   * Tests that an empty file can be created.
   */
  @Test
  public void createEmpty() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.isFile(testFile));
  }

  /**
   * Tests that create with parent creation option off throws an Exception.
   */
  @Test
  public void createNoParent() throws IOException {
    // Run the test only for local UFS. Other UFSs succeed if no parents are present
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(mUfs));

    mThrown.expect(IOException.class);
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testDir/testFile");
    OutputStream o = mUfs.create(testFile, CreateOptions.defaults().setCreateParent(false));
    o.close();
  }

  /**
   * Tests that create with parent creation option on succeeds.
   */
  @Test
  public void createParent() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testDir/testFile");
    OutputStream o = mUfs.create(testFile, CreateOptions.defaults().setCreateParent(true));
    o.close();
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
   * Tests that no bytes are read from an empty file.
   */
  @Test
  public void createOpenEmpty() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    byte[] buf = new byte[0];
    int bytesRead = mUfs.open(testFile).read(buf);
    // TODO(adit): Consider making the return value uniform across UFSs
    if (UnderFileSystemUtils.isHdfs(mUfs)) {
      Assert.assertTrue(bytesRead == -1);
    } else {
      Assert.assertTrue(bytesRead == 0);
    }
  }

  /**
   * Tests {@link UnderFileSystem#open(String, OpenOptions)} for a multi-block file.
   */
  @Test
  public void createOpenAtPosition() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    prepareMultiBlockFile(testFile);
    int[] offsets = {0, 256, 511, 512, 513, 768, 1024, 1025};
    for (int offset : offsets) {
      InputStream inputStream = mUfs.open(testFile, OpenOptions.defaults().setOffset(offset));
      Assert.assertEquals(TEST_BYTES[offset % TEST_BYTES.length], inputStream.read());
      inputStream.close();
    }
  }

  /**
   * Tests that a multi-block file can be created and validates the data written to it.
   */
  @Test
  public void createOpenLarge() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
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
          Assert.assertEquals(TEST_BYTES[(offset + i) % TEST_BYTES.length], buf[offset + i]);
        }
        offset += bytesRead;
      } else {
        ++noReadCount;
      }
    }
    Assert.assertTrue(noReadCount < 3);
  }

  /**
   * Tests seek.
   */
  @Test
  public void createOpenSeek() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    OutputStream outputStream = mUfs.create(testFile);
    int numBytes = 10;
    for (int i = 0; i < numBytes; ++i) {
      outputStream.write(i);
    }
    outputStream.close();
    InputStream inputStream = mUfs.open(testFile);
    for (int i = 0; i < numBytes; ++i) {
      ((Seekable) inputStream).seek(i);
      int readValue = inputStream.read();
      Assert.assertEquals(i, readValue);
    }
    inputStream.close();
  }

  /**
   * Tests seek when new position is going back in the opened stream.
   */
  @Test
  public void createOpenSeekReverse() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    OutputStream outputStream = mUfs.create(testFile);
    int numBytes = 10;
    for (int i = 0; i < numBytes; ++i) {
      outputStream.write(i);
    }
    outputStream.close();
    InputStream inputStream = mUfs.open(testFile);
    for (int i = numBytes - 1; i >= 0; --i) {
      ((Seekable) inputStream).seek(i);
      int readValue = inputStream.read();
      Assert.assertEquals(i, readValue);
    }
    inputStream.close();
  }

  /**
   * Tests a file can be deleted.
   */
  @Test
  public void deleteFile() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    mUfs.deleteFile(testFile);
    Assert.assertFalse(mUfs.isFile(testFile));
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
    mUfs.mkdirs(testDirEmpty, MkdirsOptions.defaults().setCreateParent(false));
    mUfs.mkdirs(testDirNonEmpty, MkdirsOptions.defaults().setCreateParent(false));
    mUfs.mkdirs(testDirNonEmptyChildDir, MkdirsOptions.defaults().setCreateParent(false));
    createEmptyFile(testDirNonEmptyChildFile);
    createEmptyFile(testDirNonEmptyChildDirFile);
    mUfs.deleteDirectory(testDirEmpty, DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(mUfs.isDirectory(testDirEmpty));
    try {
      mUfs.deleteDirectory(testDirNonEmpty, DeleteOptions.defaults().setRecursive(false));
    } catch (IOException e) {
      // Some File systems may throw IOException
    }
    Assert.assertTrue(mUfs.isDirectory(testDirNonEmpty));
    mUfs.deleteDirectory(testDirNonEmpty, DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(mUfs.isDirectory(testDirNonEmpty));
    Assert.assertFalse(mUfs.isDirectory(testDirNonEmptyChildDir));
    Assert.assertFalse(mUfs.isFile(testDirNonEmptyChildFile));
    Assert.assertFalse(mUfs.isFile(testDirNonEmptyChildDirFile));
  }

  /**
   * Tests if delete deletes all files or folders for a large directory.
   */
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
    Assert.assertFalse(mUfs.isFile(testFile));
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.isFile(testFile));
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    Assert.assertFalse(mUfs.isDirectory(testDir));
    mUfs.mkdirs(testDir, MkdirsOptions.defaults().setCreateParent(false));
    Assert.assertTrue(mUfs.isDirectory(testDir));
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
    mUfs.mkdirs(testDir, MkdirsOptions.defaults().setCreateParent(false));
    Assert.assertTrue(mUfs.isFile(testFile));
    Assert.assertFalse(mUfs.isFile(testDir));
  }

  /**
   * Tests if listStatus correctly returns file names.
   */
  @Test
  public void listStatus() throws IOException {
    String testDirNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testDirNonEmpty1");
    String testDirNonEmptyChildDir = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmpty2");
    String testDirNonEmptyChildFile = PathUtils.concatPath(testDirNonEmpty, "testDirNonEmptyF");
    String testDirNonEmptyChildDirFile =
        PathUtils.concatPath(testDirNonEmptyChildDir, "testDirNonEmptyChildDirF");
    mUfs.mkdirs(testDirNonEmpty, MkdirsOptions.defaults().setCreateParent(false));
    mUfs.mkdirs(testDirNonEmptyChildDir, MkdirsOptions.defaults().setCreateParent(false));
    createEmptyFile(testDirNonEmptyChildFile);
    createEmptyFile(testDirNonEmptyChildDirFile);
    String [] expectedResTopDir = new String[] {"testDirNonEmpty2", "testDirNonEmptyF"};
    // Some file systems may prefix with a slash
    String [] expectedResTopDir2 = new String[] {"/testDirNonEmpty2", "/testDirNonEmptyF"};
    Arrays.sort(expectedResTopDir);
    Arrays.sort(expectedResTopDir2);
    UnderFileStatus [] resTopDirStatus = mUfs.listStatus(testDirNonEmpty);
    String [] resTopDir = UnderFileStatus.convertToNames(resTopDirStatus);
    Arrays.sort(resTopDir);
    Assert.assertTrue(Arrays.equals(expectedResTopDir, resTopDir)
        || Arrays.equals(expectedResTopDir2, resTopDir));
    Assert.assertTrue(
        mUfs.listStatus(testDirNonEmptyChildDir)[0].getName().equals("testDirNonEmptyChildDirF")
            || mUfs.listStatus(testDirNonEmptyChildDir)[0].getName()
                .equals("/testDirNonEmptyChildDirF"));
    for (int i = 0; i < resTopDir.length; ++i) {
      Assert.assertEquals(
          mUfs.isDirectory(PathUtils.concatPath(testDirNonEmpty, resTopDirStatus[i].getName())),
          resTopDirStatus[i].isDirectory());
    }
  }

  /**
   * Tests if listStatus returns an empty array for an empty directory.
   */
  @Test
  public void listStatusEmpty() throws IOException {
    String testDir = PathUtils.concatPath(mUnderfsAddress, "testDir");
    mUfs.mkdirs(testDir);
    UnderFileStatus[] res = mUfs.listStatus(testDir);
    Assert.assertEquals(0, res.length);
  }

  /**
   * Tests if listStatus returns null for a file.
   */
  @Test
  public void listStatusFile() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.listStatus(testFile) == null);
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
    UnderFileStatus[] results = new UnderFileStatus[] {};
    for (int i = 0; i < 20; i++) {
      results = mUfs.listStatus(config.getTopLevelDirectory());
      if (children.length == results.length) {
        break;
      }
      CommonUtils.sleepMs(500);
    }
    Assert.assertEquals(children.length, results.length);

    String[] resultNames = UnderFileStatus.convertToNames(results);
    Arrays.sort(resultNames);
    for (int i = 0; i < children.length; ++i) {
      Assert.assertTrue(resultNames[i].equals(CommonUtils.stripPrefixIfPresent(children[i],
          PathUtils.normalizePath(config.getTopLevelDirectory(), "/"))));
    }
  }

  /**
   * Tests if list recursive correctly returns all file names in all subdirectories.
   */
  @Test
  public void listStatusRecursive() throws IOException {
    String root = mUnderfsAddress;
    // TODO(andrew): Should this directory be created in LocalAlluxioCluster creation code?
    mUfs.mkdirs(root);
    // Empty lsr should be empty
    Assert.assertEquals(0, mUfs.listStatus(root).length);

    // Create a tree of subdirectories and files
    String sub1 = PathUtils.concatPath(root, "sub1");
    String sub2 = PathUtils.concatPath(root, "sub2");
    String sub11 = PathUtils.concatPath(sub1, "sub11");
    String file11 = PathUtils.concatPath(sub11, "file11");
    String file2 = PathUtils.concatPath(sub2, "file2");
    String file = PathUtils.concatPath(root, "file");
    // lsr of nonexistent path should be null
    Assert.assertNull(mUfs.listStatus(sub1, ListOptions.defaults().setRecursive(true)));

    mUfs.mkdirs(sub1, MkdirsOptions.defaults().setCreateParent(false));
    mUfs.mkdirs(sub2, MkdirsOptions.defaults().setCreateParent(false));
    mUfs.mkdirs(sub11, MkdirsOptions.defaults().setCreateParent(false));
    createEmptyFile(file11);
    createEmptyFile(file2);
    createEmptyFile(file);

    // lsr from root should return paths relative to the root
    String[] expectedResRoot =
        {"sub1", "sub2", "sub1/sub11", "sub1/sub11/file11", "sub2/file2", "file"};
    UnderFileStatus[] actualResRootStatus =
        mUfs.listStatus(root, ListOptions.defaults().setRecursive(true));
    String[] actualResRoot = UnderFileStatus.convertToNames(actualResRootStatus);
    Arrays.sort(expectedResRoot);
    Arrays.sort(actualResRoot);
    Assert.assertArrayEquals(expectedResRoot, actualResRoot);
    for (int i = 0; i < actualResRoot.length; ++i) {
      Assert.assertEquals(
          mUfs.isDirectory(PathUtils.concatPath(root, actualResRootStatus[i].getName())),
          actualResRootStatus[i].isDirectory());
    }
    // lsr from sub1 should return paths relative to sub1
    String[] expectedResSub1 = {"sub11", "sub11/file11"};
    String[] actualResSub1 = UnderFileStatus
        .convertToNames(mUfs.listStatus(sub1, ListOptions.defaults().setRecursive(true)));
    Arrays.sort(expectedResSub1);
    Arrays.sort(actualResSub1);
    Assert.assertArrayEquals(expectedResSub1, actualResSub1);

    // lsr of file should be null
    Assert.assertNull(mUfs.listStatus(file, ListOptions.defaults().setRecursive(true)));
  }

  /**
   * Tests load metadata on list.
   */
  @Test
  public void loadMetadata() throws Exception {
    String dirName = "loadMetaDataRoot";

    String rootDir = PathUtils.concatPath(mUnderfsAddress, dirName);
    mUfs.mkdirs(rootDir);

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

  /**
   * Tests {@link UnderFileSystem#mkdirs(String)} correctly creates a directory.
   * Tests {@link UnderFileSystem#mkdirs(String, MkdirsOptions)} correctly makes parent directories
   * if createParent is specified.
   */
  @Test
  public void mkdirs() throws IOException {
    // make sure the underfs address dir exists already
    mUfs.mkdirs(mUnderfsAddress);
    // empty lsr should be empty
    Assert.assertEquals(0, mUfs.listStatus(mUnderfsAddress).length);

    String testDirTop = PathUtils.concatPath(mUnderfsAddress, "testDirTop");
    String testDir1 = PathUtils.concatPath(mUnderfsAddress, "1");
    String testDir2 = PathUtils.concatPath(testDir1, "2");
    String testDir3 = PathUtils.concatPath(testDir2, "3");
    String testDirDeep = PathUtils.concatPath(testDir3, "testDirDeep");
    mUfs.mkdirs(testDirTop, MkdirsOptions.defaults().setCreateParent(false));
    Assert.assertTrue(mUfs.isDirectory(testDirTop));
    mUfs.mkdirs(testDirDeep, MkdirsOptions.defaults().setCreateParent(true));
    Assert.assertTrue(mUfs.isDirectory(testDir1));
    Assert.assertTrue(mUfs.isDirectory(testDir2));
    Assert.assertTrue(mUfs.isDirectory(testDir3));
    Assert.assertTrue(mUfs.isDirectory(testDirDeep));
  }

  /**
   * Tests if @{@link UnderFileSystem#isDirectory(String)} infers pseudo-directories from common
   * prefixes for an object store.
   */
  @Test
  public void objectCommonPrefixesIsDirectory() throws IOException {
    // Only run test for an object store
    Assume.assumeTrue(UnderFileSystemUtils.isObjectStorage(mUfs));

    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs.getUnderFileSystem();
    ObjectStorePreConfig config = prepareObjectStore(ufs);

    String baseDirectoryPath = config.getBaseDirectoryPath();
    Assert.assertTrue(mUfs.isDirectory(baseDirectoryPath));

    for (String subDirName : config.getSubDirectoryNames()) {
      String subDirPath = PathUtils.concatPath(baseDirectoryPath, subDirName);
      Assert.assertTrue(mUfs.isDirectory(subDirPath));
    }
  }

  /**
   * Tests if a non-recursive listStatus infers pseudo-directories from common prefixes for an
   * object store.
   */
  @Test
  public void objectCommonPrefixesListStatusNonRecursive() throws IOException {
    // Only run test for an object store
    Assume.assumeTrue(UnderFileSystemUtils.isObjectStorage(mUfs));

    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs.getUnderFileSystem();
    ObjectStorePreConfig config = prepareObjectStore(ufs);

    String baseDirectoryPath = config.getBaseDirectoryPath();
    UnderFileStatus[] results = mUfs.listStatus(baseDirectoryPath);
    Assert.assertEquals(config.getSubDirectoryNames().length + config.getFileNames().length,
        results.length);
    // Check for direct children files
    for (String fileName : config.getFileNames()) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(fileName)) {
          foundIndex = i;
        }
      }
      Assert.assertTrue(foundIndex >= 0);
      Assert.assertTrue(results[foundIndex].isFile());
    }
    // Check if pseudo-directories were inferred
    for (String subDirName : config.getSubDirectoryNames()) {
      int foundIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(subDirName)) {
          foundIndex = i;
        }
      }
      Assert.assertTrue(foundIndex >= 0);
      Assert.assertTrue(results[foundIndex].isDirectory());
    }
  }

  /**
   * Tests if a recursive listStatus infers pseudo-directories from common prefixes for an object
   * store.
   */
  @Test
  public void objectCommonPrefixesListStatusRecursive() throws IOException {
    // Only run test for an object store
    Assume.assumeTrue(UnderFileSystemUtils.isObjectStorage(mUfs));

    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs.getUnderFileSystem();
    ObjectStorePreConfig config = prepareObjectStore(ufs);

    String baseDirectoryPath = config.getBaseDirectoryPath();
    UnderFileStatus[] results =
        mUfs.listStatus(baseDirectoryPath, ListOptions.defaults().setRecursive(true));
    String[] fileNames = config.getFileNames();
    String[] subDirNames = config.getSubDirectoryNames();
    Assert.assertEquals(
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
      Assert.assertTrue(foundIndex >= 0);
      Assert.assertTrue(results[foundIndex].isFile());
    }
    for (String subDirName : subDirNames) {
      // Check if pseudo-directories were inferred
      int dirIndex = -1;
      for (int i = 0; i < results.length; ++i) {
        if (results[i].getName().equals(subDirName)) {
          dirIndex = i;
        }
      }
      Assert.assertTrue(dirIndex >= 0);
      Assert.assertTrue(results[dirIndex].isDirectory());
      // Check for indirect children
      for (String fileName : config.getFileNames()) {
        int fileIndex = -1;
        for (int i = 0; i < results.length; ++i) {
          if (results[i].getName().equals(String.format("%s/%s", subDirName, fileName))) {
            fileIndex = i;
          }
        }
        Assert.assertTrue(fileIndex >= 0);
        Assert.assertTrue(results[fileIndex].isFile());
      }
    }
  }

  /**
   * Tests load metadata on list with an object store having pre-populated pseudo-directories.
   */
  @Test
  public void objectLoadMetadata() throws Exception {
    // Only run test for an object store
    Assume.assumeTrue(UnderFileSystemUtils.isObjectStorage(mUfs));

    ObjectUnderFileSystem ufs = (ObjectUnderFileSystem) mUfs.getUnderFileSystem();
    ObjectStorePreConfig config = prepareObjectStore(ufs);
    String baseDirectoryName = config.getBaseDirectoryPath()
        .substring(PathUtils.normalizePath(mUnderfsAddress, "/").length());
    AlluxioURI rootAlluxioURI = new AlluxioURI(PathUtils.concatPath("/", baseDirectoryName));
    FileSystem client = mLocalAlluxioClusterResource.get().getClient();
    List<URIStatus> results = client.listStatus(rootAlluxioURI,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));
    Assert.assertEquals(config.getSubDirectoryNames().length + config.getFileNames().length,
        results.size());
  }

  /**
   * Tests {@link UnderFileSystem#renameFile(String, String)} renames file to new location.
   */
  @Test
  public void renameFile() throws IOException {
    String testFileSrc = PathUtils.concatPath(mUnderfsAddress, "testFileSrc");
    String testFileDst = PathUtils.concatPath(mUnderfsAddress, "testFileDst");
    createEmptyFile(testFileSrc);
    mUfs.renameFile(testFileSrc, testFileDst);
    Assert.assertFalse(mUfs.isFile(testFileSrc));
    Assert.assertTrue(mUfs.isFile(testFileDst));
  }

  /**
   * Tests {@link UnderFileSystem#renameDirectory(String, String)} renames folder to new location.
   */
  @Test
  public void renameDirectory() throws IOException {
    String testDirSrc = PathUtils.concatPath(mUnderfsAddress, "testDirSrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirDst = PathUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults().setCreateParent(false));
    createEmptyFile(testDirSrcChild);
    mUfs.renameDirectory(testDirSrc, testDirDst);
    Assert.assertFalse(mUfs.isDirectory(testDirSrc));
    Assert.assertFalse(mUfs.isFile(testDirSrcChild));
    Assert.assertTrue(mUfs.isDirectory(testDirDst));
    Assert.assertTrue(mUfs.isFile(testDirDstChild));
  }

  /**
   * Tests {@link UnderFileSystem#renameDirectory(String, String)} works with nested folders.
   */
  @Test
  public void renameDirectoryDeep() throws IOException {
    String testDirSrc = PathUtils.concatPath(mUnderfsAddress, "testDirSrc");
    String testDirSrcChild = PathUtils.concatPath(testDirSrc, "testFile");
    String testDirSrcNested = PathUtils.concatPath(testDirSrc, "testNested");
    String testDirSrcNestedChild = PathUtils.concatPath(testDirSrcNested, "testNestedFile");

    String testDirDst = PathUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testDirDstChild = PathUtils.concatPath(testDirDst, "testFile");
    String testDirDstNested = PathUtils.concatPath(testDirDst, "testNested");
    String testDirDstNestedChild = PathUtils.concatPath(testDirDstNested, "testNestedFile");

    mUfs.mkdirs(testDirSrc, MkdirsOptions.defaults().setCreateParent(false));
    createEmptyFile(testDirSrcChild);
    mUfs.mkdirs(testDirSrcNested, MkdirsOptions.defaults().setCreateParent(false));
    createEmptyFile(testDirSrcNestedChild);

    mUfs.renameDirectory(testDirSrc, testDirDst);

    Assert.assertFalse(mUfs.isDirectory(testDirSrc));
    Assert.assertFalse(mUfs.isFile(testDirSrcChild));
    Assert.assertFalse(mUfs.isDirectory(testDirSrcNested));
    Assert.assertFalse(mUfs.isFile(testDirSrcNestedChild));

    Assert.assertTrue(mUfs.isDirectory(testDirDst));
    Assert.assertTrue(mUfs.isFile(testDirDstChild));
    Assert.assertTrue(mUfs.isDirectory(testDirDstNested));
    Assert.assertTrue(mUfs.isFile(testDirDstNestedChild));
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
    mUfs.mkdirs(topLevelDirectory, MkdirsOptions.defaults().setCreateParent(false));

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
      mUfs.mkdirs(children[numFiles + i], MkdirsOptions.defaults().setCreateParent(false));
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
   * Prepare a multi-block file by making copies of TEST_BYTES.
   *
   * @param testFile path of file to create
   * @return the number of copies of TEST_BYTES made
   */
  private int prepareMultiBlockFile(String testFile) throws IOException {
    OutputStream outputStream = mUfs.create(testFile);
    // Write multiple blocks of data
    int numBlocks = 3;
    // Test block size is small enough for 'int'
    int blockSize = (int) Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
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
    ObjectStorePreConfig(String baseDirectoryKey, String[] childrenFiles,
        String[] subDirectories) {
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
