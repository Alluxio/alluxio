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
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.CreateDirectoryOptions;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.util.UnderFileSystemUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests on TachyonClient (Reuse the LocalTachyonCluster).
 */
public class FileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 20000;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB,
          Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private FileSystem mTfs = null;
  private CreateFileOptions mWriteBoth;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mTfs = mLocalTachyonClusterResource.get().getClient();
    TachyonConf conf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(conf);
  }

  @Test
  public void getRootTest() throws IOException, TachyonException {
    Assert.assertEquals(0, mTfs.getStatus(new TachyonURI("/")).getFileId());
  }

  @Test
  public void createFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = 1; k < 5; k ++) {
      TachyonURI uri = new TachyonURI(uniqPath + k);
      mTfs.createFile(uri, mWriteBoth).close();
      Assert.assertNotNull(mTfs.getStatus(uri));
    }
  }

  @Test
  public void createFileWithFileAlreadyExistsExceptionTest() throws IOException, TachyonException {
    TachyonURI uri = new TachyonURI(PathUtils.uniqPath());
    mTfs.createFile(uri, mWriteBoth).close();
    Assert.assertNotNull(mTfs.getStatus(uri));
    try {
      mTfs.createFile(uri, mWriteBoth);
    } catch (TachyonException e) {
      Assert.assertEquals(e.getType(), TachyonExceptionType.FILE_ALREADY_EXISTS);
    }
  }

  @Test
  public void createFileWithInvalidPathExceptionTest() throws IOException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Path root/testFile1 is invalid.");
    mTfs.createFile(new TachyonURI("root/testFile1"), mWriteBoth);
  }

  @Test
  public void deleteFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      TachyonFSTestUtils.createByteFile(mTfs, fileURI.getPath(), k, mWriteBoth);
      Assert.assertTrue(mTfs.getStatus(fileURI).getInMemoryPercentage() == 100);
      Assert.assertNotNull(mTfs.getStatus(fileURI));
    }

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      mTfs.delete(fileURI);
      Assert.assertFalse(mTfs.exists(fileURI));
      mThrown.expect(FileDoesNotExistException.class);
      mTfs.getStatus(fileURI);
    }
  }

  @Test
  public void getFileStatusTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI(uniqPath);
    TachyonFSTestUtils.createByteFile(mTfs, uri.getPath(), writeBytes, mWriteBoth);
    Assert.assertTrue(mTfs.getStatus(uri).getInMemoryPercentage() == 100);

    Assert.assertTrue(mTfs.getStatus(uri).getPath().equals(uniqPath));
  }

  @Test
  public void mkdirTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true);
    for (int k = 0; k < 10; k ++) {
      mTfs.createDirectory(new TachyonURI(uniqPath + k), options);
      try {
        mTfs.createDirectory(new TachyonURI(uniqPath + k), options);
        Assert.fail("mkdir should throw FileAlreadyExistsException");
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
    mTfs.createFile(path1, mWriteBoth).close();
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(uniqPath + k);
      TachyonURI fileB = new TachyonURI(uniqPath + (k + 1));
      URIStatus existingFile = mTfs.getStatus(fileA);
      long oldFileId = existingFile.getFileId();
      Assert.assertNotNull(existingFile);
      mTfs.rename(fileA, fileB);
      URIStatus renamedFile = mTfs.getStatus(fileB);
      Assert.assertNotNull(renamedFile);
      Assert.assertEquals(oldFileId, renamedFile.getFileId());
    }
  }

  @Test
  public void renameFileTest2() throws IOException, TachyonException {
    TachyonURI uniqUri = new TachyonURI(PathUtils.uniqPath());
    mTfs.createFile(uniqUri, mWriteBoth).close();
    URIStatus f = mTfs.getStatus(uniqUri);
    long oldFileId = f.getFileId();
    mTfs.rename(uniqUri, uniqUri);
    Assert.assertEquals(oldFileId, mTfs.getStatus(uniqUri).getFileId());
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
      mTfs.mount(new TachyonURI("/dir1"), new TachyonURI(alternateUfsRoot));
      mTfs.loadMetadata(new TachyonURI("/dir1/file1"));
      Assert.assertEquals("file1", mTfs.listStatus(new TachyonURI("/dir1")).get(0).getName());
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

      mTfs.mount(new TachyonURI("/dirx"), new TachyonURI(dirPath1));
      mTfs.mount(new TachyonURI("/diry"), new TachyonURI(dirPath2));
      mTfs.loadMetadata(new TachyonURI("/dirx/file1"));
      mTfs.loadMetadata(new TachyonURI("/diry/file2"));
      Assert.assertEquals("file1", mTfs.listStatus(new TachyonURI("/dirx")).get(0).getName());
      Assert.assertEquals("file2", mTfs.listStatus(new TachyonURI("/diry")).get(0).getName());
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
    try {
      mTfs.mount(new TachyonURI("/dir"), new TachyonURI(ufsSubdir));
      Assert.fail("Cannot remount primary ufs.");
    } catch (IOException e) {
      // Exception expected, continue
    }

    String alternateUfsRoot = createAlternateUfs();
    try {
      String midDirPath = PathUtils.concatPath(alternateUfsRoot, "mid");
      String innerDirPath = PathUtils.concatPath(midDirPath, "inner");
      UnderFileSystemUtils.mkdirIfNotExists(innerDirPath, testConf);
      mTfs.mount(new TachyonURI("/mid"), new TachyonURI(midDirPath));
      // Cannot mount suffix of already-mounted directory
      try {
        mTfs.mount(new TachyonURI("/inner"), new TachyonURI(innerDirPath));
        Assert.fail("Cannot mount suffix of already-mounted directory");
      } catch (IOException e) {
        // Exception expected, continue
      }
      // Cannot mount prefix of already-mounted directory
      try {
        mTfs.mount(new TachyonURI("/root"), new TachyonURI(alternateUfsRoot));
        Assert.fail("Cannot mount prefix of already-mounted directory");
      } catch (IOException e) {
        // Exception expected, continue
      }
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
      mTfs.mount(new TachyonURI("/dir1"), new TachyonURI(subdirPath));
      Assert.fail("Cannot mount to path that shadows a file in the primary UFS");
    } catch (IOException e) {
      // Exception expected, continue
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }
}
