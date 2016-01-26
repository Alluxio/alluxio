/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.underfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.util.io.PathUtils;

public class UnderStorageSystemInterfaceIntegrationTest {
  private static final byte[] TEST_BYTES = "TestBytes".getBytes();

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(10000, 1000, 128);
  private String mUnderfsAddress = null;
  private UnderFileSystem mUfs = null;

  @Before
  public final void before() throws Exception {
    TachyonConf masterConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mUnderfsAddress = masterConf.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(mUnderfsAddress + TachyonURI.SEPARATOR, masterConf);
  }

  /**
   * Tests that an empty file can be created.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void createEmptyTest() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.exists(testFile));
  }

  /**
   * Tests that a file can be created and validates the data written to it.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void createOpenTest() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createTestBytesFile(testFile);
    byte[] buf = new byte[TEST_BYTES.length];
    int bytesRead = mUfs.open(testFile).read(buf);
    Assert.assertTrue(bytesRead == TEST_BYTES.length);
    Assert.assertTrue(Arrays.equals(buf, TEST_BYTES));
  }

  /**
   * Tests a file can be deleted.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void deleteFileTest() throws IOException {
    String testFile = PathUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    mUfs.delete(testFile, false);
    Assert.assertFalse(mUfs.exists(testFile));
  }

  /**
   * Tests an empty directory can be deleted.
   * Tests a non empty directory will not be deleted if recursive is not specified.
   * Tests a non empty directory will be deleted if recursive is specified.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void deleteDirTest() throws IOException {
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
   * Tests exists correctly returns true if the file exists and false if it does not.
   * Tests exists correctly returns true if the dir exists and false if it does not.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void existsTest() throws IOException {
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
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void getFileSizeTest() throws IOException {
    String testFileEmpty = PathUtils.concatPath(mUnderfsAddress, "testFileEmpty");
    String testFileNonEmpty = PathUtils.concatPath(mUnderfsAddress, "testFileNonEmpty");
    createEmptyFile(testFileEmpty);
    createTestBytesFile(testFileNonEmpty);
    Assert.assertEquals(mUfs.getFileSize(testFileEmpty), 0);
    Assert.assertEquals(mUfs.getFileSize(testFileNonEmpty), TEST_BYTES.length);
  }

  /**
   * Tests {@link UnderFileSystem#getModificationTimeMs(String)} returns a reasonably accurate time.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void getModTimeTest() throws IOException {
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
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void isFileTest() throws IOException {
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
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void listTest() throws IOException {
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
   * Tests if list recursive correctly returns all file names in all subdirectories
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void listRecursiveTest() throws IOException {
    String root = mUnderfsAddress;
    // TODO(andrew): Should this directory be created in LocalTachyonCluster creation code?
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
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void mkdirsTest() throws IOException {
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
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void renameFileTest() throws IOException {
    String testFileSrc = PathUtils.concatPath(mUnderfsAddress, "testFileSrc");
    String testFileDst = PathUtils.concatPath(mUnderfsAddress, "testFileDst");
    createEmptyFile(testFileSrc);
    mUfs.rename(testFileSrc, testFileDst);
    Assert.assertFalse(mUfs.exists(testFileSrc));
    Assert.assertTrue(mUfs.exists(testFileDst));
  }

  /**
   * Tests {@link UnderFileSystem#rename(String, String)} works file to a folder if supported.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void renameFileToFolderTest() throws IOException {
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
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void renameFolderTest() throws IOException {
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
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  @Test
  public void renameFolderToFolderTest() throws IOException {
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

  private void createEmptyFile(String path) throws IOException {
    OutputStream o = mUfs.create(path);
    o.close();
  }

  private void createTestBytesFile(String path) throws IOException {
    OutputStream o = mUfs.create(path);
    o.write(TEST_BYTES);
    o.close();
  }
}
