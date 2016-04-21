/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Integration tests on Alluxio Client (reuse the {@link LocalAlluxioCluster}).
 */
public class FileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 2 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(WORKER_CAPACITY_BYTES, Constants.MB,
          Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private FileSystem mFileSystem = null;
  private CreateFileOptions mWriteBoth;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough();
  }

  @Test
  public void getRootTest() throws IOException, AlluxioException {
    Assert.assertEquals(0, mFileSystem.getStatus(new AlluxioURI("/")).getFileId());
  }

  @Test
  public void createFileTest() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = 1; k < 5; k++) {
      AlluxioURI uri = new AlluxioURI(uniqPath + k);
      mFileSystem.createFile(uri, mWriteBoth).close();
      Assert.assertNotNull(mFileSystem.getStatus(uri));
    }
  }

  @Test
  public void createFileWithFileAlreadyExistsExceptionTest() throws IOException, AlluxioException {
    AlluxioURI uri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(uri, mWriteBoth).close();
    Assert.assertNotNull(mFileSystem.getStatus(uri));
    try {
      mFileSystem.createFile(uri, mWriteBoth);
    } catch (AlluxioException e) {
      Assert.assertTrue(e instanceof FileAlreadyExistsException);
    }
  }

  @Test
  public void createFileWithInvalidPathExceptionTest() throws IOException, AlluxioException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_INVALID.getMessage("root/testFile1"));
    mFileSystem.createFile(new AlluxioURI("root/testFile1"), mWriteBoth);
  }

  @Test
  public void deleteFileTest() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();

    for (int k = 0; k < 5; k++) {
      AlluxioURI fileURI = new AlluxioURI(uniqPath + k);
      FileSystemTestUtils.createByteFile(mFileSystem, fileURI.getPath(), k, mWriteBoth);
      Assert.assertTrue(mFileSystem.getStatus(fileURI).getInMemoryPercentage() == 100);
      Assert.assertNotNull(mFileSystem.getStatus(fileURI));
    }

    for (int k = 0; k < 5; k++) {
      AlluxioURI fileURI = new AlluxioURI(uniqPath + k);
      mFileSystem.delete(fileURI);
      Assert.assertFalse(mFileSystem.exists(fileURI));
      mThrown.expect(FileDoesNotExistException.class);
      mFileSystem.getStatus(fileURI);
    }
  }

  @Test
  public void getFileStatusTest() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    AlluxioURI uri = new AlluxioURI(uniqPath);
    FileSystemTestUtils.createByteFile(mFileSystem, uri.getPath(), writeBytes, mWriteBoth);
    Assert.assertTrue(mFileSystem.getStatus(uri).getInMemoryPercentage() == 100);

    Assert.assertTrue(mFileSystem.getStatus(uri).getPath().equals(uniqPath));
  }

  @Test
  public void mkdirTest() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true);
    for (int k = 0; k < 10; k++) {
      mFileSystem.createDirectory(new AlluxioURI(uniqPath + k), options);
      try {
        mFileSystem.createDirectory(new AlluxioURI(uniqPath + k), options);
        Assert.fail("createDirectory should throw FileAlreadyExistsException");
      } catch (FileAlreadyExistsException e) {
        Assert.assertEquals(e.getMessage(),
            ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(uniqPath + k));
      }
    }
  }

  @Test
  public void renameFileTest1() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    AlluxioURI path1 = new AlluxioURI(uniqPath + 1);
    mFileSystem.createFile(path1, mWriteBoth).close();
    for (int k = 1; k < 10; k++) {
      AlluxioURI fileA = new AlluxioURI(uniqPath + k);
      AlluxioURI fileB = new AlluxioURI(uniqPath + (k + 1));
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
  public void renameFileTest2() throws IOException, AlluxioException {
    AlluxioURI uniqUri = new AlluxioURI(PathUtils.uniqPath());
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
    AlluxioURI parentURI =
        new AlluxioURI(mLocalAlluxioClusterResource.getTestConf().get(Constants.UNDERFS_ADDRESS))
            .getParent();
    String alternateUfsRoot = parentURI.join("alternateUnderFSStorage").toString();
    UnderFileSystemUtils.mkdirIfNotExists(alternateUfsRoot,
        mLocalAlluxioClusterResource.getTestConf());
    return alternateUfsRoot;
  }

  /**
   * Deletes the alternate under file system directory.
   * @param alternateUfsRoot the root of the alternate Ufs
   * @throws IOException if an UnderFS I/O error occurs
   */
  private void destroyAlternateUfs(String alternateUfsRoot) throws IOException {
    UnderFileSystemUtils.deleteDir(alternateUfsRoot, mLocalAlluxioClusterResource.getTestConf());
  }

  @Test
  public void mountAlternateUfsTest() throws IOException, AlluxioException {
    String alternateUfsRoot = createAlternateUfs();
    try {
      String filePath = PathUtils.concatPath(alternateUfsRoot, "file1");
      UnderFileSystemUtils.touch(filePath, mLocalAlluxioClusterResource.getTestConf());
      mFileSystem.mount(new AlluxioURI("/d1"), new AlluxioURI(alternateUfsRoot));
      mFileSystem.loadMetadata(new AlluxioURI("/d1/file1"));
      Assert.assertEquals("file1", mFileSystem.listStatus(new AlluxioURI("/d1")).get(0).getName());
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }

  @Test
  public void mountAlternateUfsSubdirsTest() throws Exception {
    String alternateUfsRoot = createAlternateUfs();
    try {
      Configuration testConf = mLocalAlluxioClusterResource.getTestConf();
      String dirPath1 = PathUtils.concatPath(alternateUfsRoot, "dir1");
      String dirPath2 = PathUtils.concatPath(alternateUfsRoot, "dir2");
      UnderFileSystemUtils.mkdirIfNotExists(dirPath1, testConf);
      UnderFileSystemUtils.mkdirIfNotExists(dirPath2, testConf);
      String filePath1 = PathUtils.concatPath(dirPath1, "file1");
      String filePath2 = PathUtils.concatPath(dirPath2, "file2");
      UnderFileSystemUtils.touch(filePath1, testConf);
      UnderFileSystemUtils.touch(filePath2, testConf);

      mFileSystem.mount(new AlluxioURI("/d1"), new AlluxioURI(dirPath1));
      mFileSystem.mount(new AlluxioURI("/d2"), new AlluxioURI(dirPath2));
      mFileSystem.loadMetadata(new AlluxioURI("/d1/file1"));
      mFileSystem.loadMetadata(new AlluxioURI("/d2/file2"));
      Assert.assertEquals("file1", mFileSystem.listStatus(new AlluxioURI("/d1")).get(0).getName());
      Assert.assertEquals("file2", mFileSystem.listStatus(new AlluxioURI("/d2")).get(0).getName());
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }

  @Test
  public void mountPrefixUfsTest() throws Exception {
    Configuration testConf = mLocalAlluxioClusterResource.getTestConf();
    // Primary UFS cannot be re-mounted
    String ufsRoot = testConf.get(Constants.UNDERFS_ADDRESS);
    String ufsSubdir = PathUtils.concatPath(ufsRoot, "dir1");
    UnderFileSystemUtils.mkdirIfNotExists(ufsSubdir, testConf);
    try {
      mFileSystem.mount(new AlluxioURI("/dir"), new AlluxioURI(ufsSubdir));
      Assert.fail("Cannot remount primary ufs.");
    } catch (InvalidPathException e) {
      // Exception expected
    }

    String alternateUfsRoot = createAlternateUfs();
    try {
      String midDirPath = PathUtils.concatPath(alternateUfsRoot, "mid");
      String innerDirPath = PathUtils.concatPath(midDirPath, "inner");
      UnderFileSystemUtils.mkdirIfNotExists(innerDirPath, testConf);
      mFileSystem.mount(new AlluxioURI("/mid"), new AlluxioURI(midDirPath));
      // Cannot mount suffix of already-mounted directory
      try {
        mFileSystem.mount(new AlluxioURI("/inner"), new AlluxioURI(innerDirPath));
        Assert.fail("Cannot mount suffix of already-mounted directory");
      } catch (InvalidPathException e) {
        // Exception expected, continue
      }
      // Cannot mount prefix of already-mounted directory
      try {
        mFileSystem.mount(new AlluxioURI("/root"), new AlluxioURI(alternateUfsRoot));
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
    Configuration testConf = mLocalAlluxioClusterResource.getTestConf();
    String ufsRoot = testConf.get(Constants.UNDERFS_ADDRESS);
    String ufsSubdir = PathUtils.concatPath(ufsRoot, "dir1");
    UnderFileSystemUtils.mkdirIfNotExists(ufsSubdir, testConf);

    String alternateUfsRoot = createAlternateUfs();
    try {
      String subdirPath = PathUtils.concatPath(alternateUfsRoot, "subdir");
      UnderFileSystemUtils.mkdirIfNotExists(subdirPath, testConf);
      // Cannot mount to path that shadows a file in the primary UFS
      mFileSystem.mount(new AlluxioURI("/dir1"), new AlluxioURI(subdirPath));
      Assert.fail("Cannot mount to path that shadows a file in the primary UFS");
    } catch (IOException e) {
      // Exception expected, continue
    } finally {
      destroyAlternateUfs(alternateUfsRoot);
    }
  }
}
