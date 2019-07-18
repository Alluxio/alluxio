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

package alluxio.client.fs;

import static org.hamcrest.CoreMatchers.containsString;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
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
public class ReadOnlyMountIntegrationTest extends BaseIntegrationTest {
  private static final String MOUNT_PATH = PathUtils.concatPath("/", "mnt", "foo");
  private static final String FILE_PATH = PathUtils.concatPath(MOUNT_PATH, "file");
  private static final String SUB_DIR_PATH = PathUtils.concatPath(MOUNT_PATH, "sub", "dir");
  private static final String SUB_FILE_PATH = PathUtils.concatPath(SUB_DIR_PATH, "subfile");
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private FileSystem mFileSystem = null;
  private UnderFileSystem mUfs;
  private String mAlternateUfsRoot;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();

    // Create another directory on the local filesystem, alongside the existing Ufs, to be used as
    // a second Ufs.
    AlluxioURI parentURI =
        new AlluxioURI(ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))
            .getParent();
    mUfs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    mAlternateUfsRoot = parentURI.join("alternateUnderFSStorage").toString();
    String ufsMountDir = PathUtils.concatPath(mAlternateUfsRoot, MOUNT_PATH);
    UnderFileSystemUtils.mkdirIfNotExists(mUfs, mAlternateUfsRoot);
    UnderFileSystemUtils.mkdirIfNotExists(mUfs, ufsMountDir);
    UnderFileSystemUtils.touch(mUfs, PathUtils.concatPath(mAlternateUfsRoot, FILE_PATH));
    UnderFileSystemUtils
        .mkdirIfNotExists(mUfs, PathUtils.concatPath(mAlternateUfsRoot, SUB_DIR_PATH));
    UnderFileSystemUtils.touch(mUfs, PathUtils.concatPath(mAlternateUfsRoot, SUB_FILE_PATH));

    // Add a readonly mount point.
    mFileSystem.createDirectory(new AlluxioURI("/mnt"));
    mFileSystem.mount(new AlluxioURI(MOUNT_PATH), new AlluxioURI(ufsMountDir),
        MountPOptions.newBuilder().setReadOnly(true).build());
  }

  @After
  public void after() throws Exception {
    // Delete the alternate under file system directory.
    UnderFileSystemUtils.deleteDirIfExists(mUfs, mAlternateUfsRoot);
  }

  @Test
  public void createFile() throws IOException, AlluxioException {
    CreateFilePOptions writeBoth =
        CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();

    AlluxioURI uri = new AlluxioURI(FILE_PATH + "_create");
    try {
      mFileSystem.createFile(uri, writeBoth).close();
      Assert.fail("createFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH)));
    }

    uri = new AlluxioURI(SUB_FILE_PATH + "_create");
    try {
      mFileSystem.createFile(uri, writeBoth).close();
      Assert.fail("createFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH)));
    }
  }

  @Test
  public void createDirectory() throws IOException, AlluxioException {
    AlluxioURI uri = new AlluxioURI(PathUtils.concatPath(MOUNT_PATH, "create"));
    try {
      mFileSystem.createDirectory(uri);
      Assert.fail("createDirectory should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH)));
    }

    uri = new AlluxioURI(PathUtils.concatPath(SUB_DIR_PATH, "create"));
    try {
      mFileSystem.createDirectory(uri);
      Assert.fail("createDirectory should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH)));
    }
  }

  @Test
  public void deleteFile() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    try {
      mFileSystem.delete(fileUri);
      Assert.fail("deleteFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(fileUri, MOUNT_PATH)));
    }
    Assert.assertTrue(mFileSystem.exists(fileUri));
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    try {
      mFileSystem.delete(fileUri);
      Assert.fail("deleteFile should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(fileUri, MOUNT_PATH)));
    }
    Assert.assertTrue(mFileSystem.exists(fileUri));
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));
  }

  @Test
  public void getFileStatus() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));
  }

  @Test
  public void renameFile() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI(FILE_PATH);
    AlluxioURI dstUri = new AlluxioURI(FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH)));
    }

    srcUri = new AlluxioURI(SUB_FILE_PATH);
    dstUri = new AlluxioURI(SUB_FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH)));
    }
  }

  @Test
  public void renameFileDst() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI("/tmp");
    AlluxioURI dstUri = new AlluxioURI(FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(dstUri, MOUNT_PATH)));
    }

    dstUri = new AlluxioURI(SUB_FILE_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(dstUri, MOUNT_PATH)));
    }
  }

  @Test
  public void renameFileSrc() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI(FILE_PATH);
    AlluxioURI dstUri = new AlluxioURI("/tmp");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH)));
    }

    srcUri = new AlluxioURI(SUB_FILE_PATH);
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("rename should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH)));
    }
  }

  @Test
  public void renameDirectory() throws IOException, AlluxioException {
    AlluxioURI srcUri = new AlluxioURI(SUB_DIR_PATH);
    AlluxioURI dstUri = new AlluxioURI(SUB_DIR_PATH + "_renamed");
    try {
      mFileSystem.rename(srcUri, dstUri);
      Assert.fail("renameDirectory should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(srcUri, MOUNT_PATH)));
    }
  }

  @Test
  public void loadMetadata() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    try {
      mFileSystem.getStatus(fileUri, GetStatusPOptions.newBuilder()
          .setLoadMetadataType(LoadMetadataPType.NEVER).build());
      Assert.fail("File should not exist before loading metadata.");
    } catch (FileDoesNotExistException e) {
      Assert
          .assertEquals(e.getMessage(), ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(FILE_PATH));
    }
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    try {
      mFileSystem.getStatus(fileUri, GetStatusPOptions.newBuilder()
          .setLoadMetadataType(LoadMetadataPType.NEVER).build());
      Assert.fail("File should not exist before loading metadata.");
    } catch (FileDoesNotExistException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(SUB_FILE_PATH));
    }
    Assert.assertNotNull(mFileSystem.getStatus(fileUri));
  }

  @Test
  public void openFile() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    FileInStream inStream = mFileSystem.openFile(fileUri);
    Assert.assertNotNull(inStream);
    inStream.close();

    fileUri = new AlluxioURI(SUB_FILE_PATH);
    inStream = mFileSystem.openFile(fileUri);
    Assert.assertNotNull(inStream);
    inStream.close();
  }

  @Test
  public void setAttribute() throws IOException, AlluxioException {
    AlluxioURI fileUri = new AlluxioURI(FILE_PATH);
    mFileSystem.setAttribute(fileUri, SetAttributePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build())
        .setPinned(true)
        .setReplicationMax(10)
        .setReplicationMin(1)
        .build());
  }

  @Test
  public void chmod() throws IOException, AlluxioException {
    AlluxioURI uri = new AlluxioURI(FILE_PATH + "_chmod");
    try {
      mFileSystem.setAttribute(uri,
          SetAttributePOptions.newBuilder().setMode(new Mode((short) 0555).toProto()).build());
      Assert.fail("chomd should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH)));
    }
  }

  @Test
  public void chgrp() throws IOException, AlluxioException {
    AlluxioURI uri = new AlluxioURI(FILE_PATH + "_chgrp");
    try {
      mFileSystem.setAttribute(uri,
          SetAttributePOptions.newBuilder().setGroup("foo").build());
      Assert.fail("chgrp should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH)));
    }
  }

  @Test
  public void chown() throws IOException, AlluxioException {
    AlluxioURI uri = new AlluxioURI(FILE_PATH + "_chown");
    try {
      mFileSystem.setAttribute(uri,
          SetAttributePOptions.newBuilder().setOwner("foo").build());
      Assert.fail("chown should not succeed under a readonly mount.");
    } catch (AccessControlException e) {
      Assert.assertThat(e.getMessage(),
          containsString(ExceptionMessage.MOUNT_READONLY.getMessage(uri, MOUNT_PATH)));
    }
  }
}
