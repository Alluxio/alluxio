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
package tachyon.worker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Unit tests for tachyon.worker.WorkerStorage
 */
public class WorkerStorageTest {
  private static final long WORKER_CAPACITY_BYTES = 100000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private InetSocketAddress mMasterAddress = null;
  private NetAddress mWorkerAddress = null;
  private String mWorkerDataFolder = null;
  private ExecutorService mExecutorService;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    mExecutorService.shutdown();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mExecutorService = Executors.newFixedThreadPool(2);
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();

    mMasterAddress = mLocalTachyonCluster.getMasterAddress();
    mWorkerAddress = mLocalTachyonCluster.getWorkerAddress();
    mWorkerDataFolder = mLocalTachyonCluster.getWorkerDataFolder();
  }

  private void swapoutOrphanBlocksFileTestUtil(int filesize) throws Exception {
    int fid = TestUtils.createByteFile(mTfs, "/xyz", WriteType.MUST_CACHE, filesize);
    long bid = mTfs.getBlockId(fid, 0);
    mLocalTachyonCluster.stopWorker();
    // If you call mTfs.delete(fid, true), this will throw a java.util.concurrent.RejectedExecutionException
    // this is because stopWorker will close all clients
    // when a client is closed, you are no longer able to do any operations on it
    // so we need to get a fresh client to call delete
    mLocalTachyonCluster.getClient().delete(fid, true);

    WorkerStorage ws = new WorkerStorage(mMasterAddress, mExecutorService);
    try {
      ws.initialize(mWorkerAddress);
      String orpahnblock = ws.getUfsOrphansFolder() + TachyonURI.SEPARATOR + bid;
      UnderFileSystem ufs = UnderFileSystem.get(orpahnblock);
      StorageDir storageDir = ws.getStorageDirByBlockId(bid);
      Assert.assertFalse("Orphan block file isn't deleted from workerDataFolder", storageDir != null);
      Assert.assertTrue("UFS hasn't the orphan block file ", ufs.exists(orpahnblock));
      Assert.assertTrue("Orpahblock file size is changed", ufs.getFileSize(orpahnblock) == filesize);
    } finally {
      ws.stop();
    }
  }

  /**
   * To test the cacheBlock method when multi clients cache the same block.
   * 
   * @throws IOException
   */
  @Test
  public void cacheBlockTest() throws Exception {
    int fileLen = USER_QUOTA_UNIT_BYTES + 4;
    int fid = TestUtils.createByteFile(mTfs, "/cacheBlockTest", WriteType.THROUGH, fileLen);
    long usedBytes = mLocalTachyonCluster.getMasterInfo().getWorkersInfo().get(0).getUsedBytes();
    Assert.assertEquals(0, usedBytes);
    TachyonFS tfs1 = mLocalTachyonCluster.getClient();
    TachyonFS tfs2 = mLocalTachyonCluster.getClient();
    InStream fin1 = tfs1.getFile(fid).getInStream(ReadType.CACHE);
    InStream fin2 = tfs2.getFile(fid).getInStream(ReadType.CACHE);
    for (int i = 0; i < fileLen + 1; i ++) {
      fin1.read();
      fin2.read();
    }
    fin1.close();
    fin2.close();
    usedBytes = mLocalTachyonCluster.getMasterInfo().getWorkersInfo().get(0).getUsedBytes();
    Assert.assertEquals(fileLen, usedBytes);
  }

  /**
   * To test swapout the small file which is bigger than 64K
   * 
   * @throws Exception
   */
  @Test
  public void swapoutOrphanBlocksLargeFileTest() throws Exception {
    swapoutOrphanBlocksFileTestUtil(70000);
  }

  /**
   * To test swapout the small file which is less than 64K
   * 
   * @throws Exception
   */
  @Test
  public void swapoutOrphanBlocksSmallFileTest() throws Exception {
    swapoutOrphanBlocksFileTestUtil(10);
  }

  /**
   * To test initial WorkerStorage with unknown block files
   * 
   * @throws Exception
   */
  @Test
  public void unknownBlockFilesTest() throws Exception {
    String dirPath = System.getProperty("tachyon.worker.hierarchystore.level0.dirs.path");
    String dataFolder = CommonUtils.concat(dirPath, mWorkerDataFolder);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Wrong file name: xyz");
    mLocalTachyonCluster.stopWorker();
    // try a non-numerical file name
    File unknownFile = new File(dataFolder + TachyonURI.SEPARATOR + "xyz");
    unknownFile.createNewFile();
    WorkerStorage ws = new WorkerStorage(mMasterAddress, mExecutorService);
    try {
      ws.initialize(mWorkerAddress);
    } finally {
      ws.stop();
    }
  }
}
