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

public class UnderStorageSystemInterfaceTest {
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
