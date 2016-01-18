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

package tachyon.client;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.thrift.FileInfo;
import tachyon.util.UnderFileSystemUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests on TachyonClient (Reuse the LocalTachyonCluster).
 */
public class TachyonFileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 2 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.MB,
          Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private TachyonFileSystem mTfs = null;
  private OutStreamOptions mWriteBoth;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mTfs = mLocalTachyonClusterResource.get().getClient();
    TachyonConf conf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mWriteBoth = StreamOptionUtils.getOutStreamOptionsWriteBoth(conf);
  }

  @Test
  public void getRootTest() throws IOException, TachyonException {
    Assert.assertEquals(0, mTfs.getInfo(mTfs.open(new TachyonURI("/"))).getFileId());
  }

  @Test
  public void createFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = 1; k < 5; k ++) {
      TachyonURI uri = new TachyonURI(uniqPath + k);
      mTfs.getOutStream(uri, mWriteBoth).close();
      Assert.assertNotNull(mTfs.getInfo(mTfs.open(uri)));
    }
  }

  @Test
  public void createFileWithFileAlreadyExistsExceptionTest() throws IOException, TachyonException {
    TachyonURI uri = new TachyonURI(PathUtils.uniqPath());
    mTfs.getOutStream(uri, mWriteBoth).close();
    Assert.assertNotNull(mTfs.getInfo(mTfs.open(uri)));
    try {
      mTfs.getOutStream(uri, mWriteBoth);
    } catch (TachyonException e) {
      Assert.assertEquals(e.getType(), TachyonExceptionType.FILE_ALREADY_EXISTS);
    }
  }

  @Test
  public void createFileWithInvalidPathExceptionTest() throws IOException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_INVALID.getMessage("root/testFile1"));
    mTfs.getOutStream(new TachyonURI("root/testFile1"), mWriteBoth);
  }

  @Test
  public void deleteFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      TachyonFile f = TachyonFSTestUtils.createByteFile(mTfs, fileURI.getPath(), k, mWriteBoth);
      Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
      Assert.assertNotNull(mTfs.getInfo(f));
    }

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      TachyonFile f = mTfs.open(fileURI);
      mTfs.delete(f);
      Assert.assertNull(mTfs.openIfExists(fileURI));
      mThrown.expect(FileDoesNotExistException.class);
      mTfs.getInfo(f);
    }
  }

  @Test
  public void getFileStatusTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI(uniqPath);
    TachyonFile f = TachyonFSTestUtils.createByteFile(mTfs, uri.getPath(), writeBytes, mWriteBoth);
    Assert.assertTrue(mTfs.getInfo(f).getInMemoryPercentage() == 100);
    FileInfo fileInfo = mTfs.getInfo(f);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.getPath().equals(uniqPath));
  }

  @Test
  public void mkdirTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    MkdirOptions options = new MkdirOptions.Builder(new TachyonConf()).setRecursive(true).build();
    for (int k = 0; k < 10; k ++) {
      Assert.assertTrue(mTfs.mkdir(new TachyonURI(uniqPath + k), options));
      try {
        Assert.assertFalse(mTfs.mkdir(new TachyonURI(uniqPath + k), options));
        Assert.assertTrue("mkdir should throw FileAlreadyExistsException", false);
      } catch (FileAlreadyExistsException faee) {
        Assert.assertEquals(faee.getMessage(),
            ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(uniqPath + k));
      }
    }
  }

  @Test
  public void renameFileTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonURI path1 = new TachyonURI(uniqPath + 1);
    mTfs.getOutStream(path1, mWriteBoth).close();
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(uniqPath + k);
      TachyonURI fileB = new TachyonURI(uniqPath + (k + 1));
      TachyonFile existingFile = mTfs.open(fileA);
      long oldFileId = existingFile.getFileId();
      Assert.assertNotNull(existingFile);
      Assert.assertTrue(mTfs.rename(existingFile, fileB));
      TachyonFile renamedFile = mTfs.open(fileB);
      Assert.assertNotNull(renamedFile);
      Assert.assertEquals(oldFileId, renamedFile.getFileId());
    }
  }

  @Test
  public void renameFileTest2() throws IOException, TachyonException {
    TachyonURI uniqUri = new TachyonURI(PathUtils.uniqPath());
    mTfs.getOutStream(uniqUri, mWriteBoth).close();
    TachyonFile f = mTfs.open(uniqUri);
    long oldFileId = f.getFileId();
    Assert.assertTrue(mTfs.rename(f, uniqUri));
    Assert.assertEquals(oldFileId, mTfs.open(uniqUri).getFileId());
  }

  /**
   * Creates another directory on the local filesystem, alongside the existing Ufs, to be used as a
   * second Ufs.
   * @return the path of the alternate Ufs directory
   * @throws InvalidPathException if the UNDERFS_ADDRESS is not properly formed
   * @throws IOException if a UnderFS I/O error occurs
   */
  private String createAlternateUfs() throws InvalidPathException, IOException {
    TachyonURI parentURI =
        new TachyonURI(mLocalTachyonClusterResource.getTestConf().get(Constants.UNDERFS_ADDRESS))
            .getParent();
    String alternateUfsRoot = parentURI.join("alternateUnderFSStorage").toString();
    UnderFileSystemUtils.mkdirIfNotExists(alternateUfsRoot,
        mLocalTachyonClusterResource.getTestConf());
    return alternateUfsRoot;
  }

  /**
   * Deletes the alternate under file system directory.
   * @param alternateUfsRoot the root of the alternate Ufs
   * @throws IOException if an UnderFS I/O error occurs
   */
  private void destroyAlternateUfs(String alternateUfsRoot) throws IOException {
    UnderFileSystemUtils.deleteDir(alternateUfsRoot, mLocalTachyonClusterResource.getTestConf());
  }

  @Test
  public void mountAlternateUfsTest() throws IOException, TachyonException {
    String alternateUfsRoot = createAlternateUfs();
    try {
      String filePath = PathUtils.concatPath(alternateUfsRoot, "file1");
      UnderFileSystemUtils.touch(filePath, mLocalTachyonClusterResource.getTestConf());
      Assert.assertTrue(mTfs.mount(new TachyonURI("/dir1"), new TachyonURI(alternateUfsRoot)));
      mTfs.loadMetadata(new TachyonURI("/dir1/file1"));
      Assert.assertEquals("file1", mTfs.listStatus(mTfs.open(new TachyonURI("/dir1"))).get(0)
          .getName());
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }

  @Test
  public void mountAlternateUfsSubdirsTest() throws Exception {
    String alternateUfsRoot = createAlternateUfs();
    try {
      TachyonConf testConf = mLocalTachyonClusterResource.getTestConf();
      String dirPath1 = PathUtils.concatPath(alternateUfsRoot, "dir1");
      String dirPath2 = PathUtils.concatPath(alternateUfsRoot, "dir2");
      UnderFileSystemUtils.mkdirIfNotExists(dirPath1, testConf);
      UnderFileSystemUtils.mkdirIfNotExists(dirPath2, testConf);
      String filePath1 = PathUtils.concatPath(dirPath1, "file1");
      String filePath2 = PathUtils.concatPath(dirPath2, "file2");
      UnderFileSystemUtils.touch(filePath1, testConf);
      UnderFileSystemUtils.touch(filePath2, testConf);

      Assert.assertTrue(mTfs.mount(new TachyonURI("/dirx"), new TachyonURI(dirPath1)));
      Assert.assertTrue(mTfs.mount(new TachyonURI("/diry"), new TachyonURI(dirPath2)));
      mTfs.loadMetadata(new TachyonURI("/dirx/file1"));
      mTfs.loadMetadata(new TachyonURI("/diry/file2"));
      Assert.assertEquals("file1", mTfs.listStatus(mTfs.open(new TachyonURI("/dirx"))).get(0)
          .getName());
      Assert.assertEquals("file2", mTfs.listStatus(mTfs.open(new TachyonURI("/diry"))).get(0)
          .getName());
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }

  @Test
  public void mountPrefixUfsTest() throws Exception {
    TachyonConf testConf = mLocalTachyonClusterResource.getTestConf();
    // Primary UFS cannot be re-mounted
    String ufsRoot = testConf.get(Constants.UNDERFS_ADDRESS);
    String ufsSubdir = PathUtils.concatPath(ufsRoot, "dir1");
    UnderFileSystemUtils.mkdirIfNotExists(ufsSubdir, testConf);
    Assert.assertFalse(mTfs.mount(new TachyonURI("/dir"), new TachyonURI(ufsSubdir)));

    String alternateUfsRoot = createAlternateUfs();
    try {
      String midDirPath = PathUtils.concatPath(alternateUfsRoot, "mid");
      String innerDirPath = PathUtils.concatPath(midDirPath, "inner");
      UnderFileSystemUtils.mkdirIfNotExists(innerDirPath, testConf);
      Assert.assertTrue(mTfs.mount(new TachyonURI("/mid"), new TachyonURI(midDirPath)));
      // Cannot mount suffix of already-mounted directory
      Assert.assertFalse(mTfs.mount(new TachyonURI("/inner"), new TachyonURI(innerDirPath)));
      // Cannot mount prefix of already-mounted directory
      Assert.assertFalse(mTfs.mount(new TachyonURI("/root"), new TachyonURI(alternateUfsRoot)));
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }

  @Test
  public void mountShadowUfsTest() throws Exception {
    TachyonConf testConf = mLocalTachyonClusterResource.getTestConf();
    String ufsRoot = testConf.get(Constants.UNDERFS_ADDRESS);
    String ufsSubdir = PathUtils.concatPath(ufsRoot, "dir1");
    UnderFileSystemUtils.mkdirIfNotExists(ufsSubdir, testConf);

    String alternateUfsRoot = createAlternateUfs();
    try {
      String subdirPath = PathUtils.concatPath(alternateUfsRoot, "subdir");
      UnderFileSystemUtils.mkdirIfNotExists(subdirPath, testConf);
      // Cannot mount to path that shadows a file in the primary UFS
      Assert.assertFalse(mTfs.mount(new TachyonURI("/dir1"), new TachyonURI(subdirPath)));
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }
}
