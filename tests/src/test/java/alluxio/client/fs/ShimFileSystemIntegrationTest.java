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

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class ShimFileSystemIntegrationTest {
  private static final String SHIM_MOUNT_PATH = "/shim-mount";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  private FileSystem mFileSystem = null;
  private FileSystem mShimFileSystem = null;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    // Create a BaseFileSystem that has URI validation disabled.
    // Hadoop fs implementation, {@link ShimFileSystem} will also set this flag.
    mShimFileSystem = FileSystem.Factory.create(FileSystemContext
        .create(ClientContext.create(ServerConfiguration.global()).setDisableUriValidation(true)));
    // Mount fs-ufs for testing.
    mFileSystem.mount(new AlluxioURI(SHIM_MOUNT_PATH),
        new AlluxioURI(mTempFolder.getRoot().toURI().toString()));
  }

  @Test
  public void createFile() throws Exception {
    // File root to be mounted folder's local path.
    String foreignRoot = mTempFolder.getRoot().toURI().toString();
    // Create a foreign URI.
    AlluxioURI foreignUri = new AlluxioURI(PathUtils.concatPath(foreignRoot, PathUtils.uniqPath()));

    // Create the file with foreign URI via shim-fs.
    mShimFileSystem.createFile(foreignUri,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).setRecursive(true).build())
        .close();

    URIStatus status = mShimFileSystem.getStatus(foreignUri);
    Assert.assertNotNull(status);

    // Verify file exists via shim.
    Assert.assertTrue(mShimFileSystem.exists(foreignUri));
    // Verify file exists on Alluxio path.
    Assert.assertNotNull(mFileSystem.getStatus(new AlluxioURI(status.getPath())));
    // Verify file is persisted to ufs. (As instructed by WritePType.THROUGH).
    Assert.assertTrue(FileUtils.exists(new AlluxioURI(status.getUfsPath()).getPath()));
  }

  @Test
  public void createDirectory() throws Exception {
    // File root to be mounted folder's local path.
    String foreignRoot = mTempFolder.getRoot().toURI().toString();
    // Create a foreign URI.
    AlluxioURI foreignUri = new AlluxioURI(PathUtils.concatPath(foreignRoot, PathUtils.uniqPath()));

    // Create the file with foreign URI via shim-fs.
    mShimFileSystem.createDirectory(foreignUri, CreateDirectoryPOptions.newBuilder()
        .setWriteType(WritePType.THROUGH).setRecursive(true).build());

    URIStatus status = mShimFileSystem.getStatus(foreignUri);
    Assert.assertNotNull(status);

    // Verify the dir exists.
    Assert.assertTrue(mShimFileSystem.exists(foreignUri));
    // Verify the dir exists on Alluxio path.
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(status.getPath())));
    // Verify the dir is persisted to ufs. (As instructed by WritePType.THROUGH).
    Assert.assertTrue(FileUtils.exists(new AlluxioURI(status.getUfsPath()).getPath()));
  }

  @Test
  public void deleteFile() throws Exception {
    // File root to be mounted folder's local path.
    String foreignRoot = mTempFolder.getRoot().toURI().toString();
    // Create a foreign URI.
    AlluxioURI foreignUri = new AlluxioURI(PathUtils.concatPath(foreignRoot, PathUtils.uniqPath()));

    // Create the file with foreign URI via shim-fs.
    mShimFileSystem.createFile(foreignUri,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).setRecursive(true).build())
        .close();

    // Verify file created.
    URIStatus status = mShimFileSystem.getStatus(foreignUri);

    // Delete the file with foreign URI via shim-fs
    mShimFileSystem.delete(foreignUri, DeletePOptions.newBuilder().setAlluxioOnly(false).build());

    // Verify file is deleted.
    Assert.assertFalse(mShimFileSystem.exists(foreignUri));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI(status.getPath())));
    Assert.assertFalse(FileUtils.exists(new AlluxioURI(status.getUfsPath()).getPath()));
  }

  @Test
  public void listFiles() throws Exception {
    int testFileCount = 10;
    // File root to be mounted folder's local path.
    String foreignRoot = mTempFolder.getRoot().toURI().toString();
    for (int i = 0; i < testFileCount; i++) {
      // Create a foreign URI.
      AlluxioURI foreignUri =
          new AlluxioURI(PathUtils.concatPath(foreignRoot, Integer.toString(i)));
      // Create the file with foreign URI via shim-fs.
      mShimFileSystem.createFile(foreignUri, CreateFilePOptions.newBuilder()
          .setWriteType(WritePType.THROUGH).setRecursive(true).build()).close();
    }

    // List files via shim-fs.
    List<URIStatus> shimStatusList = mShimFileSystem.listStatus(new AlluxioURI(foreignRoot),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    Assert.assertEquals(testFileCount, shimStatusList.size());
    // List files via Alluxio-fs.
    // Get foreign root status to get Alluxio path.
    URIStatus dirStatus = mShimFileSystem.getStatus(new AlluxioURI(foreignRoot));
    List<URIStatus> statusList = mFileSystem.listStatus(new AlluxioURI(dirStatus.getPath()));
    Assert.assertEquals(testFileCount, statusList.size());
  }

  @Test
  public void renameFile() throws Exception {
    // File root to be mounted folder's local path.
    String foreignRoot = mTempFolder.getRoot().toURI().toString();
    // Create a foreign URI.
    AlluxioURI foreignUriSrc =
        new AlluxioURI(PathUtils.concatPath(foreignRoot, PathUtils.uniqPath()));
    AlluxioURI foreignUriDst =
        new AlluxioURI(PathUtils.concatPath(foreignRoot, PathUtils.uniqPath()));

    // Create the file with foreign URI via shim-fs.
    mShimFileSystem.createFile(foreignUriSrc,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).setRecursive(true).build())
        .close();
    URIStatus srcStatus = mShimFileSystem.getStatus(foreignUriSrc);

    // Rename the file with foreign URIs via shim-fs.
    mShimFileSystem.rename(foreignUriSrc, foreignUriDst);
    URIStatus dstStatus = mShimFileSystem.getStatus(foreignUriDst);

    // Verify the file is moved to destination.
    Assert.assertTrue(mShimFileSystem.exists(foreignUriDst));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(dstStatus.getPath())));
    Assert.assertTrue(FileUtils.exists(new AlluxioURI(dstStatus.getUfsPath()).getPath()));
    // Verify the source is gone.
    Assert.assertFalse(mShimFileSystem.exists(foreignUriSrc));
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI(srcStatus.getPath())));
    Assert.assertFalse(FileUtils.exists(new AlluxioURI(srcStatus.getUfsPath()).getPath()));
  }

  @Test
  public void setAttribute() throws Exception {
    // File root to be mounted folder's local path.
    String foreignRoot = mTempFolder.getRoot().toURI().toString();
    // Create a foreign URI.
    AlluxioURI foreignUri = new AlluxioURI(PathUtils.concatPath(foreignRoot, PathUtils.uniqPath()));

    // Create the file with foreign URI via shim-fs.
    mShimFileSystem.createFile(foreignUri,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).setRecursive(true).build())
        .close();

    // Change file attributes via shim-fs.
    mShimFileSystem.setAttribute(foreignUri,
        SetAttributePOptions.newBuilder().setPinned(true).build());

    // Verify attribute change took place.
    Assert.assertTrue(mShimFileSystem.getStatus(foreignUri).isPinned());
  }

  @Test
  public void mountNotExist() throws Exception {
    // Bogus foreign root.
    String foreignRoot = "file:///bogus";
    // Create a foreign URI.
    AlluxioURI foreignUri = new AlluxioURI(PathUtils.concatPath(foreignRoot, PathUtils.uniqPath()));

    // Create the file with foreign URI via shim-fs.
    // This should fail as the foreign root is not mounted on Alluxio.
    mException.expect(InvalidPathException.class);
    mShimFileSystem.createFile(foreignUri,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).setRecursive(true).build())
        .close();
  }
}
