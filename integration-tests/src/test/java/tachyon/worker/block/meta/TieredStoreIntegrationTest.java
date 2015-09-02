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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.client.next.file.FileInStream;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for {@link tachyon.worker.block.meta.StorageTier}.
 */
public class TieredStoreIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private LocalTachyonCluster mLocalTachyonCluster;
  private TachyonConf mWorkerConf;
  private TachyonFS mTFS;
  private int mWorkerToMasterHeartbeatIntervalMs;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster =
        new LocalTachyonCluster(MEM_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB);
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
    mWorkerConf = mLocalTachyonCluster.getWorkerTachyonConf();
    mWorkerToMasterHeartbeatIntervalMs =
        mWorkerConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
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
    FileInStream in = mTFS.getFile(fileId).getInStream(ReadType.NO_CACHE);

    // Delete the file
    mTFS.delete(fileId, false);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the delete, the master should no longer serve the file
    Assert.assertFalse(mTFS.exist(new TachyonURI("/test1")));

    // However, the previous read should still be able to read it as the data still exists
    byte[] res = new byte[MEM_CAPACITY_BYTES];
    Assert.assertEquals(MEM_CAPACITY_BYTES, in.read(res, 0, MEM_CAPACITY_BYTES));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(MEM_CAPACITY_BYTES, res));
    in.close();

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the file is closed, the master's delete should go through and new files can be created
    int newFile = TachyonFSTestUtils.createByteFile(mTFS, "/test2", WriteType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);
    Assert.assertTrue(mTFS.getFile(newFile).isInMemory());
  }

  // Tests that pinning a file prevents it from being evicted.
  @Test
  public void pinFileTest() throws Exception {
    // Create a file that fills the entire Tachyon store
    int fileId =
        TachyonFSTestUtils.createByteFile(mTFS, "/test1", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Pin the file
    mTFS.pinFile(fileId);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the pin with master
    Assert.assertTrue(mTFS.getFileStatus(fileId, false).isIsPinned());

    // Try to create a file that cannot be stored unless the previous file is evicted, expect an
    // exception since worker cannot serve the request
    mThrown.expect(IOException.class);
    TachyonFSTestUtils.createByteFile(mTFS, "/test2", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
  }

  // Tests that pinning a file and then unpinning
  @Test
  public void unpinFileTest() throws Exception {
    // Create a file that fills the entire Tachyon store
    int fileId1 =
        TachyonFSTestUtils.createByteFile(mTFS, "/test1", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Pin the file
    mTFS.pinFile(fileId1);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the pin with master
    Assert.assertTrue(mTFS.getFileStatus(fileId1, false).isIsPinned());

    // Unpin the file
    mTFS.unpinFile(fileId1);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the unpin
    Assert.assertFalse(mTFS.getFileStatus(fileId1, false).isIsPinned());

    // Try to create a file that cannot be stored unless the previous file is evicted, this
    // should succeed
    int fileId2 =
        TachyonFSTestUtils.createByteFile(mTFS, "/test2", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // File 2 should be in memory and File 1 should be evicted
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);
    Assert.assertFalse(mTFS.getFile(fileId1).isInMemory());
    Assert.assertTrue(mTFS.getFile(fileId2).isInMemory());
  }

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

    CommonUtils.sleepMs(null, CommonUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 2 + 10);

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

    CommonUtils.sleepMs(null, CommonUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 2 + 10);

    Assert.assertEquals(MEM_CAPACITY_BYTES / 6, len);
    Assert.assertEquals(true, file1.isInMemory());
    Assert.assertEquals(false, file2.isInMemory());
    Assert.assertEquals(true, file3.isInMemory());
    Assert.assertEquals(MEM_CAPACITY_BYTES / 6 + MEM_CAPACITY_BYTES,
        mLocalTachyonCluster.getMasterInfo().getUsedBytes());
  }
  */
}
