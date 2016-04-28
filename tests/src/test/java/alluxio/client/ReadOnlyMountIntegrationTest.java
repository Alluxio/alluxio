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
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Integration tests for mounting (reuse the {@link LocalAlluxioCluster}).
 */
public class ReadOnlyMountIntegrationTest {
  private static final String MOUNT_PATH = PathUtils.concatPath("/", "mnt", "foo");
  private static final String FILE_PATH = PathUtils.concatPath(MOUNT_PATH, "file");
  private static final String SUB_DIR_PATH = PathUtils.concatPath(MOUNT_PATH, "sub", "dir");
  private static final String SUB_FILE_PATH = PathUtils.concatPath(SUB_DIR_PATH, "subfile");
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
  private FileSystem mFileSystem = null;

  private String mAlternateUfsRoot;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    Configuration testConf = mLocalAlluxioClusterResource.getTestConf();

    // Add a readonly mount point.
    mAlternateUfsRoot = createAlternateUfs();
    String ufsMountDir = PathUtils.concatPath(mAlternateUfsRoot, MOUNT_PATH);
    UnderFileSystemUtils.mkdirIfNotExists(ufsMountDir, testConf);
    UnderFileSystemUtils.touch(PathUtils.concatPath(mAlternateUfsRoot, FILE_PATH), testConf);
    UnderFileSystemUtils
        .mkdirIfNotExists(PathUtils.concatPath(mAlternateUfsRoot, SUB_DIR_PATH), testConf);
    UnderFileSystemUtils
        .touch(PathUtils.concatPath(mAlternateUfsRoot, SUB_FILE_PATH), testConf);
    mFileSystem.createDirectory(new AlluxioURI("/mnt"));
    mFileSystem.mount(new AlluxioURI(MOUNT_PATH), new AlluxioURI(ufsMountDir),
        MountOptions.defaults().setReadOnly(true));
  }

  @After
  public void after() throws Exception {
    destroyAlternateUfs(mAlternateUfsRoot);
  }

  @Test
  public void createFileTest() throws IOException, AlluxioException {
    CreateFileOptions writeBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough();

    AlluxioURI uri = new AlluxioURI(FILE_PATH + "_create");
    try {
      mFileSystem.createFile(uri, writeBoth).close();
      Assert.fail("createFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH));
    }

    uri = new AlluxioURI(SUB_FILE_PATH + "_create");
    try {
      mFileSystem.createFile(uri, writeBoth).close();
      Assert.fail("createFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH));
    }
  }

  @Test
  public void createDirectoryTest() throws IOException, AlluxioException {
    AlluxioURI uri = new AlluxioURI(PathUtils.concatPath(MOUNT_PATH, "create"));
    try {
      mFileSystem.createDirectory(uri);
      Assert.fail("createDirectory should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH));
    }

    uri = new AlluxioURI(PathUtils.concatPath(SUB_DIR_PATH, "create"));
    try {
      mFileSystem.createDirectory(uri);
      Assert.fail("createDirectory should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH));
    }
  }

  @Test
  public void deleteFileTest() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    mFileSystem.loadMetadata(fileUri);
    try {
      mFileSystem.delete(fileUri);
      Assert.fail("deleteFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(fileUri, MOUNT_PATH));
    }
    Assert.assertTrue(mFileSystem.exists(fileUri));
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    mFileSystem.loadMetadata(fileUri, LoadMetadataOptions.defaults().setRecursive(true));
    try {
      mFileSystem.delete(fileUri);
      Assert.fail("deleteFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(fileUri, MOUNT_PATH));
    }
    Assert.assertTrue(mFileSystem.exists(fileUri));
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));
  }

  @Test
  public void getFileStatusTest() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    mFileSystem.loadMetadata(fileUri);
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    mFileSystem.loadMetadata(fileUri, LoadMetadataOptions.defaults().setRecursive(true));
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));
  }

  @Test
  public void renameFileTest() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI(FILE_PATH);
    AlluxioURI dstUri = new AlluxioURI(FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH));
    }

    srcUri = new AlluxioURI(SUB_FILE_PATH);
    dstUri = new AlluxioURI(SUB_FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH));
    }
  }

  @Test
  public void renameFileDstTest() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI("/tmp");
    AlluxioURI dstUri = new AlluxioURI(FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(dstUri, MOUNT_PATH));
    }

    dstUri = new AlluxioURI(SUB_FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(dstUri, MOUNT_PATH));
    }
  }

  @Test
  public void renameFileSrcTest() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI(FILE_PATH);
    AlluxioURI dstUri = new AlluxioURI("/tmp");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH));
    }

    srcUri = new AlluxioURI(SUB_FILE_PATH);
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH));
    }
  }

  @Test
  public void renameDirectoryTest() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI(SUB_DIR_PATH);
    AlluxioURI dstUri = new AlluxioURI(SUB_DIR_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("renameDirectory should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH));
    }
  }

  @Test
  public void loadMetadataTest() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    // TODO(jiri) Re-enable this once we support the "check UFS" option for getStatus.
//    try {
//      mFileSystem.getStatus(fileUri);
//      Assert.fail("File should not exist before loading metadata.");
//    } catch (FileDoesNotExistException e) {
//      Assert
//        .assertEquals(e.getMessage(), ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(FILE_PATH));
//    }
    mFileSystem.loadMetadata(fileUri);
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    // TODO(jiri) Re-enable this once we support the "check UFS" option for getStatus.
//    try {
//      mFileSystem.getStatus(fileUri);
//      Assert.fail("File should not exist before loading metadata.");
//    } catch (FileDoesNotExistException e) {
//      Assert.assertEquals(e.getMessage(),
//          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(SUB_FILE_PATH));
//    }
    mFileSystem.loadMetadata(fileUri, LoadMetadataOptions.defaults().setRecursive(true));
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));
  }

  @Test
  public void openFileTest() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    mFileSystem.loadMetadata(fileUri);
    Assert.assertNotNull(mFileSystem.openFile(fileUri));

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    mFileSystem.loadMetadata(fileUri, LoadMetadataOptions.defaults().setRecursive(true));
    Assert.assertNotNull(mFileSystem.openFile(fileUri));
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
    UnderFileSystemUtils
        .mkdirIfNotExists(alternateUfsRoot, mLocalAlluxioClusterResource.getTestConf());
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
}
