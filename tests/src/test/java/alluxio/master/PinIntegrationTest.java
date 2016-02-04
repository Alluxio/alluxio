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

import alluxio.LocalTachyonClusterResource;
import alluxio.TachyonURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.TachyonException;
import alluxio.worker.file.FileSystemMasterClient;

public final class PinIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource();
  private FileSystem mFileSystem = null;
  private FileSystemMasterClient mFSMasterClient;
  private SetAttributeOptions mSetPinned;
  private SetAttributeOptions mUnsetPinned;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalTachyonClusterResource.get().getClient();
    mFSMasterClient = new FileSystemMasterClient(
        new InetSocketAddress(mLocalTachyonClusterResource.get().getMasterHostname(),
            mLocalTachyonClusterResource.get().getMasterPort()),
        mLocalTachyonClusterResource.get().getWorkerTachyonConf());
    mSetPinned = SetAttributeOptions.defaults().setPinned(true);
    mUnsetPinned = SetAttributeOptions.defaults().setPinned(false);
  }

  @After
  public final void after() throws Exception {
    mFSMasterClient.close();
  }

  @Test
  public void recursivePinness() throws Exception {
    TachyonURI folderURI = new TachyonURI("/myFolder");
    TachyonURI fileURI = new TachyonURI("/myFolder/myFile");

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
    mFileSystem.setAttribute(new TachyonURI("/"), mSetPinned);

    // Child file should be pinned
    TachyonURI file0 = new TachyonURI("/file0");
    createEmptyFile(file0);
    URIStatus status0 = mFileSystem.getStatus(file0);
    Assert.assertTrue(status0.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // Child folder should be pinned
    TachyonURI folder = new TachyonURI("/folder");
    mFileSystem.createDirectory(folder);
    Assert.assertTrue(mFileSystem.getStatus(folder).isPinned());

    // Grandchild file also pinned
    TachyonURI file1 = new TachyonURI("/folder/file1");
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
    createEmptyFile(new TachyonURI("/folder/file2"));
    Assert.assertFalse(mFileSystem.getStatus(new TachyonURI("/folder/file2")).isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // But top level children still should be pinned!
    createEmptyFile(new TachyonURI("/file3"));
    URIStatus status3 = mFileSystem.getStatus(new TachyonURI("/file3"));
    Assert.assertTrue(status3.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId(), status3.getFileId()));
  }

  private void createEmptyFile(TachyonURI fileURI) throws IOException, TachyonException {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    FileOutStream os = mFileSystem.createFile(fileURI, options);
    os.close();
  }
}
