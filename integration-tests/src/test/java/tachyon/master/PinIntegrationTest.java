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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.WorkerFileSystemMasterClient;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.ThreadFactoryUtils;

public class PinIntegrationTest {
  private final ExecutorService mExecutorService =
      Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("test-executor-%d", true));

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private WorkerFileSystemMasterClient mFSMasterClient;
  private SetStateOptions mSetPinned;
  private SetStateOptions mUnsetPinned;

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mFSMasterClient = new WorkerFileSystemMasterClient(
        new InetSocketAddress(mLocalTachyonCluster.getMasterHostname(),
            mLocalTachyonCluster.getMasterPort()),
        mExecutorService, mLocalTachyonCluster.getWorkerTachyonConf());
    mSetPinned = new SetStateOptions.Builder(new TachyonConf()).setPinned(true).build();
    mUnsetPinned = new SetStateOptions.Builder(new TachyonConf()).setPinned(false).build();
  }

  @After
  public final void after() throws Exception {
    mFSMasterClient.close();
    mLocalTachyonCluster.stop();
  }

  @Test
  public void recursivePinness() throws Exception {
    TachyonFile dir0 = mTfs.open(new TachyonURI("/"));
    TachyonURI folderURI = new TachyonURI("/myFolder");
    TachyonURI fileURI = new TachyonURI("/myFolder/myFile");

    mTfs.mkdir(folderURI);
    TachyonFile dir = mTfs.open(folderURI);

    TachyonFile file = createEmptyFile(fileURI);
    Assert.assertFalse(mTfs.getInfo(file).isPinned);

    mTfs.setState(file, mSetPinned);
    Assert.assertTrue(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file.getFileId()));

    mTfs.setState(file, mUnsetPinned);
    Assert.assertFalse(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Integer>newHashSet());

    // Pinning a folder should recursively pin subfolders.
    mTfs.setState(dir, mSetPinned);
    Assert.assertTrue(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file.getFileId()));

    // Same with unpinning.
    mTfs.setState(dir0, mUnsetPinned);
    Assert.assertFalse(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Integer>newHashSet());

    // The last pin command always wins.
    mTfs.setState(file, mSetPinned);
    Assert.assertTrue(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file.getFileId()));
  }

  @Test
  public void newFilesInheritPinness() throws Exception {
    // Children should inherit the isPinned value of their parents on creation.

    // Pin root
    TachyonFile root = mTfs.open(new TachyonURI("/"));
    mTfs.setState(root, mSetPinned);

    // Child file should be pinned
    TachyonFile file0 = createEmptyFile(new TachyonURI("/file0"));
    Assert.assertTrue(mTfs.getInfo(file0).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file0.getFileId()));

    // Child folder should be pinned
    mTfs.mkdir(new TachyonURI("/folder"));
    TachyonFile folder = mTfs.open(new TachyonURI("/folder"));
    Assert.assertTrue(mTfs.getInfo(folder).isPinned);

    // Grandchild file also pinned
    TachyonFile file1 = createEmptyFile(new TachyonURI("/folder/file1"));
    Assert.assertTrue(mTfs.getInfo(file1).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file0.getFileId(), file1.getFileId()));

    // Unpinning child folder should cause its children to be unpinned as well
    mTfs.setState(folder, mUnsetPinned);
    Assert.assertFalse(mTfs.getInfo(folder).isPinned);
    Assert.assertFalse(mTfs.getInfo(file1).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file0.getFileId()));

    // And new grandchildren should be unpinned too.
    TachyonFile file2 = createEmptyFile(new TachyonURI("/folder/file2"));
    Assert.assertFalse(mTfs.getInfo(file2).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file0.getFileId()));

    // But toplevel children still should be pinned!
    TachyonFile file3 = createEmptyFile(new TachyonURI("/file3"));
    Assert.assertTrue(mTfs.getInfo(file3).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file0.getFileId(), file3.getFileId()));
  }

  private TachyonFile createEmptyFile(TachyonURI fileURI) throws IOException, TachyonException {
    OutStreamOptions options =
        new OutStreamOptions.Builder(new TachyonConf())
            .setTachyonStorageType(TachyonStorageType.STORE)
            .setUnderStorageType(UnderStorageType.NO_PERSIST).build();
    FileOutStream os = mTfs.getOutStream(fileURI, options);
    os.close();
    return mTfs.open(fileURI);
  }
}
