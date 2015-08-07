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

package tachyon.worker.block.meta;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;

/**
 * Integration tests for {@link tachyon.worker.block.meta.StorageTier}.
 */
public class TieredStoreIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 1000;
  private static final int DISK_CAPACITY_BYTES = 10000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;
  private TachyonConf mWorkerConf;
  private int mWorkerToMasterHeartbeatIntervalMs;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.worker.tieredstore.level.max");
    System.clearProperty("tachyon.worker.tieredstore.level1.alias");
    System.clearProperty("tachyon.worker.tieredstore.level1.dirs.path");
    System.clearProperty("tachyon.worker.tieredstore.level1.dirs.quota");
  }

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster =
        new LocalTachyonCluster(MEM_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB);

    // Add system properties to pre-populate the storage tiers
    // TODO Need to change LocalTachyonCluster to pass this info to be set in TachyonConf
    System.setProperty("tachyon.worker.tieredstore.level.max", "2");
    System.setProperty("tachyon.worker.tieredstore.level1.alias", "HDD");
    System.setProperty("tachyon.worker.tieredstore.level1.dirs.path", "/disk1" + "," + "/disk2");
    System.setProperty("tachyon.worker.tieredstore.level1.dirs.quota", DISK_CAPACITY_BYTES + "");

    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
    mWorkerConf = mLocalTachyonCluster.getWorkerTachyonConf();
    mWorkerToMasterHeartbeatIntervalMs =
        mWorkerConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS);
  }

  // Tests that deletes go through despite failing initially due to concurrent read
  @Test
  public void deleteWhileReadTest() throws Exception {
    int fileId =
        TachyonFSTestUtils.createByteFile(mTFS, "/test1", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
    TachyonFile file = mTFS.getFile(fileId);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    Assert.assertTrue(file.isInMemory());
    // Open the file
    InStream in = mTFS.getFile(fileId).getInStream(ReadType.NO_CACHE);

    // Delete the file
    mTFS.delete(fileId, false);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the delete, the master should no longer serve the file
    Assert.assertFalse(mTFS.exist(new TachyonURI("/test1")));

    // However, the previous read should still be able to read it as the data still exists
    byte[] res = new byte[MEM_CAPACITY_BYTES];
    Assert.assertEquals(MEM_CAPACITY_BYTES, in.read(res, 0, MEM_CAPACITY_BYTES));
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(MEM_CAPACITY_BYTES, res));
    in.close();

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the file is closed, the master's delete should go through and new files can be created
    int newFile = TachyonFSTestUtils.createByteFile(mTFS, "/test2", WriteType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);
    Assert.assertTrue(mTFS.getFile(newFile).isInMemory());
  }

  // TODO: this test is allocator and evictor specific and really testing LRU.
  /*@Test
  public void blockEvict() throws IOException, InterruptedException {
    int fileId1 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test1", WriteType.TRY_CACHE,
            MEM_CAPACITY_BYTES / 6);
    int fileId2 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test2", WriteType.TRY_CACHE,
            MEM_CAPACITY_BYTES / 6);
    int fileId3 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test3", WriteType.TRY_CACHE,
            MEM_CAPACITY_BYTES / 6);

    TachyonFile file1 = mTFS.getFile(fileId1);
    TachyonFile file2 = mTFS.getFile(fileId2);
    TachyonFile file3 = mTFS.getFile(fileId3);

    Assert.assertEquals(file1.isInMemory(), true);
    Assert.assertEquals(file2.isInMemory(), true);
    Assert.assertEquals(file3.isInMemory(), true);

    int fileId4 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test4", WriteType.TRY_CACHE,
            MEM_CAPACITY_BYTES / 2);
    int fileId5 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test5", WriteType.MUST_CACHE,
            MEM_CAPACITY_BYTES / 2);

    CommonUtils.sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 2 + 10);

    TachyonFile file4 = mTFS.getFile(fileId4);
    TachyonFile file5 = mTFS.getFile(fileId5);
    LOG.info("file1 {}", file1.isInMemory());
    LOG.info("file2 {}", file2.isInMemory());
    LOG.info("file3 {}", file3.isInMemory());
    LOG.info("file4 {}", file4.isInMemory());
    LOG.info("file5 {}", file5.isInMemory());

    Assert.assertEquals(file1.isInMemory(), false);
    Assert.assertEquals(file2.isInMemory(), false);
    Assert.assertEquals(file3.isInMemory(), false);
    Assert.assertEquals(file4.isInMemory(), true);
    Assert.assertEquals(file5.isInMemory(), true);
  }
  */

  // TODO: Add this test back when CACHE_PROMOTE is enabled again
  /*
  @Test
  public void promoteBlock() throws IOException, InterruptedException {
    int fileId1 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test1", WriteType.TRY_CACHE,
            MEM_CAPACITY_BYTES / 6);
    int fileId2 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test2", WriteType.TRY_CACHE,
            MEM_CAPACITY_BYTES / 2);
    int fileId3 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test3", WriteType.TRY_CACHE,
            MEM_CAPACITY_BYTES / 2);

    CommonUtils.sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 2 + 10);

    TachyonFile file1 = mTFS.getFile(fileId1);
    TachyonFile file2 = mTFS.getFile(fileId2);
    TachyonFile file3 = mTFS.getFile(fileId3);

    Assert.assertEquals(false, file1.isInMemory());
    Assert.assertEquals(true, file2.isInMemory());
    Assert.assertEquals(true, file3.isInMemory());
    Assert.assertEquals(MEM_CAPACITY_BYTES / 6 + MEM_CAPACITY_BYTES,
        mLocalTachyonCluster.getMasterInfo().getUsedBytes());

    InStream is = file1.getInStream(ReadType.CACHE_PROMOTE);
    byte[] buf = new byte[MEM_CAPACITY_BYTES / 6];
    int len = is.read(buf);
    is.close();

    CommonUtils.sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 2 + 10);

    Assert.assertEquals(MEM_CAPACITY_BYTES / 6, len);
    Assert.assertEquals(true, file1.isInMemory());
    Assert.assertEquals(false, file2.isInMemory());
    Assert.assertEquals(true, file3.isInMemory());
    Assert.assertEquals(MEM_CAPACITY_BYTES / 6 + MEM_CAPACITY_BYTES,
        mLocalTachyonCluster.getMasterInfo().getUsedBytes());
  }
  */
}
