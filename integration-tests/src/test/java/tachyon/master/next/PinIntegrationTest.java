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

package tachyon.master.next;

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
import tachyon.client.FileSystemMasterClient;
import tachyon.client.next.CacheType;
import tachyon.client.next.ClientOptions;
import tachyon.client.next.UnderStorageType;
import tachyon.client.next.file.FileOutStream;
import tachyon.client.next.file.TachyonFileSystem;
import tachyon.client.next.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;

public class PinIntegrationTest {
  private final ExecutorService mExecutorService =
      Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("test-executor-%d", true));

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private FileSystemMasterClient mFSMasterClient;

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mFSMasterClient = new FileSystemMasterClient(
        new InetSocketAddress(mLocalTachyonCluster.getMasterHostname(),
            mLocalTachyonCluster.getMasterPort()),
        mExecutorService, mLocalTachyonCluster.getWorkerTachyonConf());
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

    mTfs.mkdirs(folderURI);
    TachyonFile dir = mTfs.open(folderURI);

    TachyonFile file = createEmptyFile(fileURI);
    Assert.assertFalse(mTfs.getInfo(file).isPinned);

    mTfs.setPin(file, true);
    Assert.assertTrue(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file.getFileId()));

    mTfs.setPin(file, false);
    Assert.assertFalse(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Integer>newHashSet());

    // Pinning a folder should recursively pin subfolders.
    mTfs.setPin(dir, true);
    Assert.assertTrue(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file.getFileId()));

    // Same with unpinning.
    mTfs.setPin(dir0, false);
    Assert.assertFalse(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()), Sets.<Integer>newHashSet());

    // The last pin command always wins.
    mTfs.setPin(file, true);
    Assert.assertTrue(mTfs.getInfo(file).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file.getFileId()));
  }

  @Test
  public void newFilesInheritPinness() throws Exception {
    // Children should inherit the isPinned value of their parents on creation.

    // Pin root
    TachyonFile root = mTfs.open(new TachyonURI("/"));
    mTfs.setPin(root, true);

    // Child file should be pinned
    TachyonFile file0 = createEmptyFile(new TachyonURI("/file0"));
    Assert.assertTrue(mTfs.getInfo(file0).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file0.getFileId()));

    // Child folder should be pinned
    mTfs.mkdirs(new TachyonURI("/folder"));
    TachyonFile folder = mTfs.open(new TachyonURI("/folder"));
    Assert.assertTrue(mTfs.getInfo(folder).isPinned);

    // Grandchild file also pinned
    TachyonFile file1 = createEmptyFile(new TachyonURI("/folder/file1"));
    Assert.assertTrue(mTfs.getInfo(file1).isPinned);
    Assert.assertEquals(Sets.newHashSet(mFSMasterClient.getPinList()),
        Sets.newHashSet(file0.getFileId(), file1.getFileId()));

    // Unpinning child folder should cause its children to be unpinned as well
    mTfs.setPin(folder, false);
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

  private TachyonFile createEmptyFile(TachyonURI fileURI) throws IOException {
    ClientOptions options = new ClientOptions.Builder(new TachyonConf())
        .setCacheType(CacheType.CACHE).setUnderStorageType(UnderStorageType.NO_PERSIST).build();
    FileOutStream os = mTfs.getOutStream(fileURI, options);
    os.close();
    return mTfs.open(fileURI);
  }
}
