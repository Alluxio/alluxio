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
import alluxio.client.WriteType;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.master.MasterClientConfig;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;
import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

/**
 * Integration tests for the pin operation.
 */
public final class PinIntegrationTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH").build();
  private FileSystem mFileSystem = null;
  private FileSystemMasterClient mFSMasterClient;
  private SetAttributeOptions mSetPinned;
  private SetAttributeOptions mUnsetPinned;
  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  private static final CommonOptions SYNC_ALWAYS =
      CommonOptions.defaults().setSyncIntervalMs(0);
  private static final CommonOptions SYNC_NEVER =
      CommonOptions.defaults().setSyncIntervalMs(-1);

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mFSMasterClient = new FileSystemMasterClient(MasterClientConfig.defaults());
    mSetPinned = SetAttributeOptions.defaults().setPinned(true);
    mUnsetPinned = SetAttributeOptions.defaults().setPinned(false);
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI(mLocalUfsPath));
  }

  @After
  public final void after() throws Exception {
    mFSMasterClient.close();
  }

  /**
   * Tests that pinning/unpinning a folder should recursively take effect on its subfolders.
   */
  @Test
  public void recursivePinness() throws Exception {
    AlluxioURI folderURI = new AlluxioURI("/myFolder");
    AlluxioURI fileURI = new AlluxioURI("/myFolder/myFile");

    mFileSystem.createDirectory(folderURI);

    createEmptyFile(fileURI);
    Assert.assertFalse(mFileSystem.getStatus(fileURI).isPinned());

    mFileSystem.setAttribute(fileURI, mSetPinned);
    URIStatus status = mFileSystem.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));

    mFileSystem.setAttribute(fileURI, mUnsetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertFalse(status.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()), new HashSet<Long>());

    // Pinning a folder should recursively pin subfolders.
    mFileSystem.setAttribute(folderURI, mSetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));

    // Same with unpinning.
    mFileSystem.setAttribute(folderURI, mUnsetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertFalse(status.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()), new HashSet<Long>());

    // The last pin command always wins.
    mFileSystem.setAttribute(fileURI, mSetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));
  }

  /**
   * Tests that children should inherit the isPinned attribute from their parent on creation.
   */
  @Test
  public void newFilesInheritPinness() throws Exception {
    // Pin root
    mFileSystem.setAttribute(new AlluxioURI("/"), mSetPinned);

    // Child file should be pinned
    AlluxioURI file0 = new AlluxioURI("/file0");
    createEmptyFile(file0);
    URIStatus status0 = mFileSystem.getStatus(file0);
    Assert.assertTrue(status0.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // Child folder should be pinned
    AlluxioURI folder = new AlluxioURI("/folder");
    mFileSystem.createDirectory(folder);
    Assert.assertTrue(mFileSystem.getStatus(folder).isPinned());

    // Grandchild file also pinned
    AlluxioURI file1 = new AlluxioURI("/folder/file1");
    createEmptyFile(file1);
    URIStatus status1 = mFileSystem.getStatus(file1);
    Assert.assertTrue(status1.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId(), status1.getFileId()));

    // Unpinning child folder should cause its children to be unpinned as well
    mFileSystem.setAttribute(folder, mUnsetPinned);
    Assert.assertFalse(mFileSystem.getStatus(folder).isPinned());
    Assert.assertFalse(mFileSystem.getStatus(file1).isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // And new grandchildren should be unpinned too.
    createEmptyFile(new AlluxioURI("/folder/file2"));
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI("/folder/file2")).isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // But top level children still should be pinned!
    createEmptyFile(new AlluxioURI("/file3"));
    URIStatus status3 = mFileSystem.getStatus(new AlluxioURI("/file3"));
    Assert.assertTrue(status3.isPinned());
    Assert.assertEquals(new HashSet<>(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId(), status3.getFileId()));
  }

  /**
   * Make sure Pinning and Unpinning would recursively sync the directory if ufs sync is on.
   */
  @Test
  public void pinDiscoverNewFiles() throws Exception {
    String deeplyNestedDir = "/tmp/tmp2/tmp3";

    // Create a dir
    new File(ufsPath(deeplyNestedDir)).mkdirs();
    // Write a file in UFS
    FileWriter fileWriter = new FileWriter(ufsPath(PathUtils.concatPath(deeplyNestedDir,
        "/newfile")));
    fileWriter.write("test");
    fileWriter.close();

    SetAttributeOptions attributeOption = SetAttributeOptions.defaults()
        .setPinned(true).setCommonOptions(SYNC_ALWAYS);
    GetStatusOptions getStatusOption = GetStatusOptions.defaults()
        .setCommonOptions(SYNC_NEVER);
    // Pin the dir
    mFileSystem.setAttribute(new AlluxioURI("/mnt/tmp/"), attributeOption);
    Configuration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.Never.toString());
    URIStatus dirStat = mFileSystem.getStatus(new AlluxioURI("/mnt/tmp/"), getStatusOption);
    URIStatus fileStat = mFileSystem.getStatus(new AlluxioURI(PathUtils.concatPath("/mnt" ,
        deeplyNestedDir, "newfile")), getStatusOption);
    Assert.assertTrue(dirStat.isPinned());
    Assert.assertTrue(fileStat.isPinned());
  }

  private String ufsPath(String path) {
    return PathUtils.concatPath(mLocalUfsPath, path);
  }

  private void createEmptyFile(AlluxioURI fileURI) throws IOException, AlluxioException {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    FileOutStream os = mFileSystem.createFile(fileURI, options);
    os.close();
  }
}
