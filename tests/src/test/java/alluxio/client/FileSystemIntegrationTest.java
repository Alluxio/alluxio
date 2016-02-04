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

package alluxio.client;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import alluxio.Constants;
import alluxio.LocalTachyonClusterResource;
import alluxio.TachyonURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.conf.TachyonConf;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.TachyonException;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

/**
 * Integration tests on TachyonClient (Reuse the LocalTachyonCluster).
 */
public class FileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 2 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES, Constants.MB,
          Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private FileSystem mFileSystem = null;
  private CreateFileOptions mWriteBoth;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalTachyonClusterResource.get().getClient();
    TachyonConf conf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(conf);
  }

  @Test
  public void getRootTest() throws IOException, TachyonException {
    Assert.assertEquals(0, mFileSystem.getStatus(new TachyonURI("/")).getFileId());
  }

  @Test
  public void createFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = 1; k < 5; k ++) {
      TachyonURI uri = new TachyonURI(uniqPath + k);
      mFileSystem.createFile(uri, mWriteBoth).close();
      Assert.assertNotNull(mFileSystem.getStatus(uri));
    }
  }

  @Test
  public void createFileWithFileAlreadyExistsExceptionTest() throws IOException, TachyonException {
    TachyonURI uri = new TachyonURI(PathUtils.uniqPath());
    mFileSystem.createFile(uri, mWriteBoth).close();
    Assert.assertNotNull(mFileSystem.getStatus(uri));
    try {
      mFileSystem.createFile(uri, mWriteBoth);
    } catch (TachyonException e) {
      Assert.assertTrue(e instanceof FileAlreadyExistsException);
    }
  }

  @Test
  public void createFileWithInvalidPathExceptionTest() throws IOException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_INVALID.getMessage("root/testFile1"));
    mFileSystem.createFile(new TachyonURI("root/testFile1"), mWriteBoth);
  }

  @Test
  public void deleteFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      FileSystemTestUtils.createByteFile(mFileSystem, fileURI.getPath(), k, mWriteBoth);
      Assert.assertTrue(mFileSystem.getStatus(fileURI).getInMemoryPercentage() == 100);
      Assert.assertNotNull(mFileSystem.getStatus(fileURI));
    }

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      mFileSystem.delete(fileURI);
      Assert.assertFalse(mFileSystem.exists(fileURI));
      mThrown.expect(FileDoesNotExistException.class);
      mFileSystem.getStatus(fileURI);
    }
  }

  @Test
  public void getFileStatusTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri.getPath(), writeBytes, mWriteBoth);
    Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);

    Assert.assertTrue(mFileSystem.getStatus(uri).getPath().equals(uniqPath));
  }

  @Test
  public void mkdirTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true);
    for (int k = 0; k < 10; k ++) {
      mFileSystem.createDirectory(new TachyonURI(uniqPath + k), options);
      try {
        mFileSystem.createDirectory(new TachyonURI(uniqPath + k), options);
        Assert.fail("mkdir should throw FileAlreadyExistsException");
      } catch (FileAlreadyExistsException e) {
        Assert.assertEquals(e.getMessage(),
            ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(uniqPath + k));
      }
    }
  }

  @Test
  public void renameFileTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonURI path1 = new TachyonURI(uniqPath + 1);
    mFileSystem.createFile(path1, mWriteBoth).close();
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(uniqPath + k);
      TachyonURI fileB = new TachyonURI(uniqPath + (k + 1));
      URIStatus existingFile = mFileSystem.getStatus(fileA);
      long oldFileId = existingFile.getFileId();
      Assert.assertNotNull(existingFile);
      mFileSystem.rename(fileA, fileB);
      URIStatus renamedFile = mFileSystem.getStatus(fileB);
      Assert.assertNotNull(renamedFile);
      Assert.assertEquals(oldFileId, renamedFile.getFileId());
    }
  }

  @Test
  public void renameFileTest2() throws IOException, TachyonException {
    TachyonURI uniqUri = new TachyonURI(PathUtils.uniqPath());
    mFileSystem.createFile(uniqUri, mWriteBoth).close();
    URIStatus f = mFileSystem.getStatus(uniqUri);
    long oldFileId = f.getFileId();
    mFileSystem.rename(uniqUri, uniqUri);
    Assert.assertEquals(oldFileId, mFileSystem.getStatus(uniqUri).getFileId());
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
      mFileSystem.mount(new TachyonURI("/d1"), new TachyonURI(alternateUfsRoot));
      mFileSystem.loadMetadata(new TachyonURI("/d1/file1"));
      Assert.assertEquals("file1", mFileSystem.listStatus(new TachyonURI("/d1")).get(0).getName());
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

      mFileSystem.mount(new TachyonURI("/d1"), new TachyonURI(dirPath1));
      mFileSystem.mount(new TachyonURI("/d2"), new TachyonURI(dirPath2));
      mFileSystem.loadMetadata(new TachyonURI("/d1/file1"));
      mFileSystem.loadMetadata(new TachyonURI("/d2/file2"));
      Assert.assertEquals("file1", mFileSystem.listStatus(new TachyonURI("/d1")).get(0).getName());
      Assert.assertEquals("file2", mFileSystem.listStatus(new TachyonURI("/d2")).get(0).getName());
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
      mFileSystem.mount(new TachyonURI("/dir"), new TachyonURI(ufsSubdir));
      Assert.fail("Cannot remount primary ufs.");
    } catch (InvalidPathException e) {
      // Exception expected
    }

    String alternateUfsRoot = createAlternateUfs();
    try {
      String midDirPath = PathUtils.concatPath(alternateUfsRoot, "mid");
      String innerDirPath = PathUtils.concatPath(midDirPath, "inner");
      UnderFileSystemUtils.mkdirIfNotExists(innerDirPath, testConf);
      mFileSystem.mount(new TachyonURI("/mid"), new TachyonURI(midDirPath));
      // Cannot mount suffix of already-mounted directory
      try {
        mFileSystem.mount(new TachyonURI("/inner"), new TachyonURI(innerDirPath));
        Assert.fail("Cannot mount suffix of already-mounted directory");
      } catch (InvalidPathException e) {
        // Exception expected, continue
      }
      // Cannot mount prefix of already-mounted directory
      try {
        mFileSystem.mount(new TachyonURI("/root"), new TachyonURI(alternateUfsRoot));
        Assert.fail("Cannot mount prefix of already-mounted directory");
      } catch (InvalidPathException e) {
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
      mFileSystem.mount(new TachyonURI("/dir1"), new TachyonURI(subdirPath));
      Assert.fail("Cannot mount to path that shadows a file in the primary UFS");
    } catch (IOException e) {
      // Exception expected, continue
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }
}
