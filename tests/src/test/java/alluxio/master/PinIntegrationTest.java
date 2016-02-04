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

package alluxio.master;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Sets;

import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.worker.file.FileSystemMasterClient;

public final class PinIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
  private FileSystem mFileSystem = null;
  private FileSystemMasterClient mFSMasterClient;
  private SetAttributeOptions mSetPinned;
  private SetAttributeOptions mUnsetPinned;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mFSMasterClient = new FileSystemMasterClient(
        new InetSocketAddress(mLocalAlluxioClusterResource.get().getMasterHostname(),
            mLocalAlluxioClusterResource.get().getMasterPort()),
        mLocalAlluxioClusterResource.get().getWorkerTachyonConf());
    mSetPinned = SetAttributeOptions.defaults().setPinned(true);
    mUnsetPinned = SetAttributeOptions.defaults().setPinned(false);
  }

  @After
  public final void after() throws Exception {
    mFSMasterClient.close();
  }

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
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));

    mFileSystem.setAttribute(fileURI, mUnsetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertFalse(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Long>newHashSet());

    // Pinning a folder should recursively pin subfolders.
    mFileSystem.setAttribute(folderURI, mSetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));

    // Same with unpinning.
    mFileSystem.setAttribute(folderURI, mUnsetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertFalse(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Long>newHashSet());

    // The last pin command always wins.
    mFileSystem.setAttribute(fileURI, mSetPinned);
    status = mFileSystem.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));
  }

  @Test
  public void newFilesInheritPinness() throws Exception {
    // Children should inherit the isPinned value of their parents on creation.

    // Pin root
    mFileSystem.setAttribute(new AlluxioURI("/"), mSetPinned);

    // Child file should be pinned
    AlluxioURI file0 = new AlluxioURI("/file0");
    createEmptyFile(file0);
    URIStatus status0 = mFileSystem.getStatus(file0);
    Assert.assertTrue(status0.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
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
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId(), status1.getFileId()));

    // Unpinning child folder should cause its children to be unpinned as well
    mFileSystem.setAttribute(folder, mUnsetPinned);
    Assert.assertFalse(mFileSystem.getStatus(folder).isPinned());
    Assert.assertFalse(mFileSystem.getStatus(file1).isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // And new grandchildren should be unpinned too.
    createEmptyFile(new AlluxioURI("/folder/file2"));
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI("/folder/file2")).isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // But top level children still should be pinned!
    createEmptyFile(new AlluxioURI("/file3"));
    URIStatus status3 = mFileSystem.getStatus(new AlluxioURI("/file3"));
    Assert.assertTrue(status3.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId(), status3.getFileId()));
  }

  private void createEmptyFile(AlluxioURI fileURI) throws IOException, AlluxioException {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    FileOutStream os = mFileSystem.createFile(fileURI, options);
    os.close();
  }
}
