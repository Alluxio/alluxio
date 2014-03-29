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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.worker.WorkerStorage;

/**
 * Unit tests for tachyon.WorkerStorage
 */
public class WorkerStorageTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private InetSocketAddress mMasterAddress = null;
  private InetSocketAddress mWorkerAddress = null;
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

    WorkerStorage ws =
        new WorkerStorage(mMasterAddress, mWorkerAddress, mWorkerDataFolder, WORKER_CAPACITY_BYTES);
    String orpahnblock = ws.getUnderfsOrphansFolder() + Constants.PATH_SEPARATOR + bid;
    UnderFileSystem ufs = UnderFileSystem.get(orpahnblock);
    Assert.assertFalse("Orphan block file isn't deleted from workerDataFolder", new File(
        mWorkerDataFolder + Constants.PATH_SEPARATOR + bid).exists());
    Assert.assertTrue("UFS hasn't the orphan block file ", ufs.exists(orpahnblock));
    Assert.assertTrue("Orpahblock file size is changed", ufs.getFileSize(orpahnblock) == filesize);
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
    new WorkerStorage(mMasterAddress, mWorkerAddress, mWorkerDataFolder, WORKER_CAPACITY_BYTES);
  }
}
