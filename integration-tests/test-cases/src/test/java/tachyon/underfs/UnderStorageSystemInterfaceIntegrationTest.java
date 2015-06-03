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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;

public class UnderStorageSystemInterfaceIntegrationTest {
  private static final byte[] TEST_BYTES = "TestBytes".getBytes();

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private String mUnderfsAddress = null;
  private UnderFileSystem mUfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(10000, 1000, 128);
    mLocalTachyonCluster.start();
    TachyonConf masterConf = mLocalTachyonCluster.getMasterTachyonConf();
    mUnderfsAddress = masterConf.get(Constants.UNDERFS_ADDRESS, null);
    mUfs = UnderFileSystem.get(mUnderfsAddress + TachyonURI.SEPARATOR, masterConf);
  }

  // Tests that an empty file can be created
  @Test
  public void createEmptyTest() throws IOException {
    String testFile = CommonUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.exists(testFile));
  }

  // Tests that a file can be created and validates the data written to it
  @Test
  public void createOpenTest() throws IOException {
    String testFile = CommonUtils.concatPath(mUnderfsAddress, "testFile");
    createTestBytesFile(testFile);
    byte[] buf = new byte[TEST_BYTES.length];
    int bytesRead = mUfs.open(testFile).read(buf);
    Assert.assertTrue(bytesRead == TEST_BYTES.length);
    Assert.assertTrue(Arrays.equals(buf, TEST_BYTES));
  }

  // Tests a file can be deleted
  @Test
  public void deleteFileTest() throws IOException {
    String testFile = CommonUtils.concatPath(mUnderfsAddress, "testFile");
    createEmptyFile(testFile);
    mUfs.delete(testFile, false);
    Assert.assertFalse(mUfs.exists(testFile));
  }

  // Tests an empty directory can be deleted
  // Tests a non empty directory will not be deleted if recursive is not specified
  // Tests a non empty directory will be deleted if recursive is specified
  @Test
  public void deleteDirTest() throws IOException {
    String testDirEmpty = CommonUtils.concatPath(mUnderfsAddress, "testDirEmpty");
    String testDirNonEmpty = CommonUtils.concatPath(mUnderfsAddress, "testDirNonEmpty1");
    String testDirNonEmptyChildDir = CommonUtils.concatPath(testDirNonEmpty, "testDirNonEmpty2");
    String testDirNonEmptyChildFile = CommonUtils.concatPath(testDirNonEmpty, "testDirNonEmptyF");
    String testDirNonEmptyChildDirFile = CommonUtils.concatPath(testDirNonEmptyChildDir,
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
    } catch (IOException ioe) {
      // Some File systems may throw IOException
    }
    Assert.assertTrue(mUfs.exists(testDirNonEmpty));
    mUfs.delete(testDirNonEmpty, true);
    Assert.assertFalse(mUfs.exists(testDirNonEmpty));
    Assert.assertFalse(mUfs.exists(testDirNonEmptyChildDir));
    Assert.assertFalse(mUfs.exists(testDirNonEmptyChildFile));
    Assert.assertFalse(mUfs.exists(testDirNonEmptyChildDirFile));
  }

  // Tests exists correctly returns true if the file exists and false if it does not
  // Tests exists correctly returns true if the dir exists and false if it does not
  @Test
  public void testExists() throws IOException {
    String testFile = CommonUtils.concatPath(mUnderfsAddress, "testFile");
    Assert.assertFalse(mUfs.exists(testFile));
    createEmptyFile(testFile);
    Assert.assertTrue(mUfs.exists(testFile));
    String testDir = CommonUtils.concatPath(mUnderfsAddress, "testDir");
    Assert.assertFalse(mUfs.exists(testDir));
    mUfs.mkdirs(testDir, false);
    Assert.assertTrue(mUfs.exists(testDir));
  }

  // Tests getFileSize correctly returns the file size
  @Test
  public void testGetFileSize() throws IOException {
    String testFileEmpty = CommonUtils.concatPath(mUnderfsAddress, "testFileEmpty");
    String testFileNonEmpty = CommonUtils.concatPath(mUnderfsAddress, "testFileNonEmpty");
    createEmptyFile(testFileEmpty);
    createTestBytesFile(testFileNonEmpty);
    Assert.assertEquals(mUfs.getFileSize(testFileEmpty), 0);
    Assert.assertEquals(mUfs.getFileSize(testFileNonEmpty), TEST_BYTES.length);
  }

  // Tests getModificationTimeMs returns a reasonably accurate time
  @Test
  public void testGetModTime() throws IOException {
    long slack = 1000; // Some file systems may report nearest second.
    long start = System.currentTimeMillis();
    String testFile = CommonUtils.concatPath(mUnderfsAddress, "testFile");
    createTestBytesFile(testFile);
    long end = System.currentTimeMillis();
    long modTime = mUfs.getModificationTimeMs(testFile);
    Assert.assertTrue(modTime >= start - slack);
    Assert.assertTrue(modTime <= end + slack);
  }

  // Tests if isFile correctly returns true for files and false otherwise
  @Test
  public void testIsFile() throws IOException {
    String testFile = CommonUtils.concatPath(mUnderfsAddress, "testFile");
    String testDir = CommonUtils.concatPath(mUnderfsAddress, "testDir");
    Assert.assertFalse(mUfs.isFile(testFile));
    createEmptyFile(testFile);
    mUfs.mkdirs(testDir, false);
    Assert.assertTrue(mUfs.isFile(testFile));
    Assert.assertFalse(mUfs.isFile(testDir));
  }

  // Tests if list correctly returns file names
  @Test
  public void testList() throws IOException {
    String testDirNonEmpty = CommonUtils.concatPath(mUnderfsAddress, "testDirNonEmpty1");
    String testDirNonEmptyChildDir = CommonUtils.concatPath(testDirNonEmpty, "testDirNonEmpty2");
    String testDirNonEmptyChildFile = CommonUtils.concatPath(testDirNonEmpty, "testDirNonEmptyF");
    String testDirNonEmptyChildDirFile =
        CommonUtils.concatPath(testDirNonEmptyChildDir, "testDirNonEmptyChildDirF");
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

  // Tests mkdirs correctly creates a directory
  // Tests mkdirs correctly makes parent directories if createParent is specified
  @Test
  public void testMkdirs() throws IOException {
    String testDirTop = CommonUtils.concatPath(mUnderfsAddress, "testDirTop");
    String testDir1 = CommonUtils.concatPath(mUnderfsAddress, "1");
    String testDir2 = CommonUtils.concatPath(testDir1, "2");
    String testDir3 = CommonUtils.concatPath(testDir2, "3");
    String testDirDeep = CommonUtils.concatPath(testDir3, "testDirDeep");
    mUfs.mkdirs(testDirTop, false);
    Assert.assertTrue(mUfs.exists(testDirTop));
    mUfs.mkdirs(testDirDeep, true);
    Assert.assertTrue(mUfs.exists(testDir1));
    Assert.assertTrue(mUfs.exists(testDir2));
    Assert.assertTrue(mUfs.exists(testDir3));
    Assert.assertTrue(mUfs.exists(testDirDeep));
  }

  // Tests rename works file to new location
  @Test
  public void testRenameFile() throws IOException {
    String testFileSrc = CommonUtils.concatPath(mUnderfsAddress, "testFileSrc");
    String testFileDst = CommonUtils.concatPath(mUnderfsAddress, "testFileDst");
    createEmptyFile(testFileSrc);
    mUfs.rename(testFileSrc, testFileDst);
    Assert.assertFalse(mUfs.exists(testFileSrc));
    Assert.assertTrue(mUfs.exists(testFileDst));
  }

  // Tests rename works file to a folder if supported
  @Test
  public void testRenameFileToFolder() throws IOException {
    String testFileSrc = CommonUtils.concatPath(mUnderfsAddress, "testFileSrc");
    String testFileDst = CommonUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testFileFinalDst = CommonUtils.concatPath(testFileDst, "testFileSrc");
    createEmptyFile(testFileSrc);
    mUfs.mkdirs(testFileDst, false);
    if (mUfs.rename(testFileSrc, testFileDst)) {
      Assert.assertFalse(mUfs.exists(testFileSrc));
      Assert.assertTrue(mUfs.exists(testFileFinalDst));
    }
  }

  // Tests rename works folder to new location
  @Test
  public void testRenameFolder() throws IOException {
    String testDirSrc = CommonUtils.concatPath(mUnderfsAddress, "testDirSrc");
    String testDirSrcChild = CommonUtils.concatPath(testDirSrc, "testFile");
    String testDirDst = CommonUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testDirDstChild = CommonUtils.concatPath(testDirDst, "testFile");
    mUfs.mkdirs(testDirSrc, false);
    createEmptyFile(testDirSrcChild);
    mUfs.rename(testDirSrc, testDirDst);
    Assert.assertFalse(mUfs.exists(testDirSrc));
    Assert.assertFalse(mUfs.exists(testDirSrcChild));
    Assert.assertTrue(mUfs.exists(testDirDst));
    Assert.assertTrue(mUfs.exists(testDirDstChild));
  }

  // Tests rename works folder to another folder if supported
  @Test
  public void testRenameFolderToFolder() throws IOException {
    String testDirSrc = CommonUtils.concatPath(mUnderfsAddress, "testDirSrc");
    String testDirSrcChild = CommonUtils.concatPath(testDirSrc, "testFile");
    String testDirDst = CommonUtils.concatPath(mUnderfsAddress, "testDirDst");
    String testDirDstChild = CommonUtils.concatPath(testDirDst, "testFile");
    String testDirFinalDst = CommonUtils.concatPath(testDirDst, "testDirSrc");
    String testDirChildFinalDst = CommonUtils.concatPath(testDirFinalDst, "testFile");
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
