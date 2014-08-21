/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.WorkerStorage
 */
public class WorkerStorageTest {
  class ConcurrentCacheBlock implements Callable<Void> {
    private int fid;
    private int fileLen;

    ConcurrentCacheBlock(int fid, int fileLen) {
      this.fid = fid;
      this.fileLen = fileLen;
    }

    @Override
    public Void call() throws Exception {
      exec(this.fid, this.fileLen);
      return null;
    }

    public void exec(int fid, int fileLen) throws Exception {
      byte[] content = new byte[fileLen];
      TachyonFS tfs = mLocalTachyonCluster.getClient();
      tfs.getFile(fid).getInStream(ReadType.CACHE).read(content);
    }
  }

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private InetSocketAddress mMasterAddress = null;
  private NetAddress mWorkerAddress = null;
  private String mWorkerDataFolder = null;

  private final long WORKER_CAPACITY_BYTES = 100000;
  private final int USER_QUOTA_UNIT_BYTES = 100;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
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
    mTfs.delete(fid, true);

    WorkerStorage ws = new WorkerStorage(mMasterAddress, mWorkerDataFolder, WORKER_CAPACITY_BYTES);
    ws.initialize(mWorkerAddress);
    String orpahnblock = ws.getUfsOrphansFolder() + Constants.PATH_SEPARATOR + bid;
    UnderFileSystem ufs = UnderFileSystem.get(orpahnblock);
    Assert.assertFalse("Orphan block file isn't deleted from workerDataFolder", new File(
        mWorkerDataFolder + Constants.PATH_SEPARATOR + bid).exists());
    Assert.assertTrue("UFS hasn't the orphan block file ", ufs.exists(orpahnblock));
    Assert.assertTrue("Orpahblock file size is changed", ufs.getFileSize(orpahnblock) == filesize);
  }

  /**
   * To test the cacheBlock method when multi clients cache the same block concurrently.
   * 
   * @throws IOException
   */
  @Test
  public void cacheBlockTest() throws Exception {
    int fileLen = USER_QUOTA_UNIT_BYTES + 1000;

    // In order to detect whether there exists some random bugs, we reconduct the concurrent tests
    // in a loop here
    for (int round = 0; round < 10; round ++) {
      int fid = TestUtils.createByteFile(mTfs, "/cacheBlockTest", WriteType.THROUGH, fileLen);
      long usedBytes = mLocalTachyonCluster.getMasterInfo().getWorkersInfo().get(0).getUsedBytes();
      Assert.assertEquals(0, usedBytes);
      ExecutorService executor = Executors.newCachedThreadPool();
      ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(5);
      for (int i = 0; i < 5; i ++) {
        Callable<Void> call = new ConcurrentCacheBlock(fid, fileLen);
        futures.add(executor.submit(call));
      }
      for (Future<Void> f : futures) {
        f.get();
      }
      executor.shutdown();
      usedBytes = mLocalTachyonCluster.getMasterInfo().getWorkersInfo().get(0).getUsedBytes();
      Assert.assertEquals(fileLen, usedBytes);
      mTfs.delete(fid, false);
    }
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
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Wrong file name: xyz");
    mLocalTachyonCluster.stopWorker();
    // try a non-numerical file name
    File unknownFile = new File(mWorkerDataFolder + Constants.PATH_SEPARATOR + "xyz");
    unknownFile.createNewFile();
    WorkerStorage ws = new WorkerStorage(mMasterAddress, mWorkerDataFolder, WORKER_CAPACITY_BYTES);
    ws.initialize(mWorkerAddress);
  }
}
