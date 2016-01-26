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

package tachyon.master;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.WriteType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.exception.TachyonException;
import tachyon.worker.file.FileSystemMasterClient;

public class PinIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(1000, 1000, Constants.GB);
  private FileSystem mTfs = null;
  private FileSystemMasterClient mFSMasterClient;
  private SetAttributeOptions mSetPinned;
  private SetAttributeOptions mUnsetPinned;

  @Before
  public final void before() throws Exception {
    mTfs = mLocalTachyonClusterResource.get().getClient();
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

    mTfs.createDirectory(folderURI);

    createEmptyFile(fileURI);
    Assert.assertFalse(mTfs.getStatus(fileURI).isPinned());

    mTfs.setAttribute(fileURI, mSetPinned);
    URIStatus status = mTfs.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));

    mTfs.setAttribute(fileURI, mUnsetPinned);
    status = mTfs.getStatus(fileURI);
    Assert.assertFalse(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Long>newHashSet());

    // Pinning a folder should recursively pin subfolders.
    mTfs.setAttribute(folderURI, mSetPinned);
    status = mTfs.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));

    // Same with unpinning.
    mTfs.setAttribute(folderURI, mUnsetPinned);
    status = mTfs.getStatus(fileURI);
    Assert.assertFalse(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Long>newHashSet());

    // The last pin command always wins.
    mTfs.setAttribute(fileURI, mSetPinned);
    status = mTfs.getStatus(fileURI);
    Assert.assertTrue(status.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status.getFileId()));
  }

  @Test
  public void newFilesInheritPinness() throws Exception {
    // Children should inherit the isPinned value of their parents on creation.

    // Pin root
    mTfs.setAttribute(new TachyonURI("/"), mSetPinned);

    // Child file should be pinned
    TachyonURI file0 = new TachyonURI("/file0");
    createEmptyFile(file0);
    URIStatus status0 = mTfs.getStatus(file0);
    Assert.assertTrue(status0.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // Child folder should be pinned
    TachyonURI folder = new TachyonURI("/folder");
    mTfs.createDirectory(folder);
    Assert.assertTrue(mTfs.getStatus(folder).isPinned());

    // Grandchild file also pinned
    TachyonURI file1 = new TachyonURI("/folder/file1");
    createEmptyFile(file1);
    URIStatus status1 = mTfs.getStatus(file1);
    Assert.assertTrue(status1.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId(), status1.getFileId()));

    // Unpinning child folder should cause its children to be unpinned as well
    mTfs.setAttribute(folder, mUnsetPinned);
    Assert.assertFalse(mTfs.getStatus(folder).isPinned());
    Assert.assertFalse(mTfs.getStatus(file1).isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // And new grandchildren should be unpinned too.
    createEmptyFile(new TachyonURI("/folder/file2"));
    Assert.assertFalse(mTfs.getStatus(new TachyonURI("/folder/file2")).isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId()));

    // But top level children still should be pinned!
    createEmptyFile(new TachyonURI("/file3"));
    URIStatus status3 = mTfs.getStatus(new TachyonURI("/file3"));
    Assert.assertTrue(status3.isPinned());
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(status0.getFileId(), status3.getFileId()));
  }

  private void createEmptyFile(TachyonURI fileURI) throws IOException, TachyonException {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    FileOutStream os = mTfs.createFile(fileURI, options);
    os.close();
  }
}
