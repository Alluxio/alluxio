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
import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;

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
  }

  @Test
  public void pinFileTest() throws Exception {
    // Create a file and then pin it
    int fileId1 =
        TachyonFSTestUtils.createByteFile(mTFS, "/test1", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
    mTFS.pinFile(fileId1);
    CommonUtils.sleepMs(LOG, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 3);

    // Try to create a file that cannot be stored unless the previous file is evicted, expect an
    // exception since worker cannot serve the request
    Exception caughtException = null;
    try {
      TachyonFSTestUtils.createByteFile(mTFS, "/test2", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);
    } catch (IOException ioe) {
      caughtException = ioe;
    }
    Assert.assertNotNull(caughtException);

    // Unpin the first file
    mTFS.unpinFile(fileId1);
    CommonUtils.sleepMs(LOG, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 3);

    // The create should now succeed
    int fileId3 =
        TachyonFSTestUtils.createByteFile(mTFS, "/test3", WriteType.MUST_CACHE, MEM_CAPACITY_BYTES);

    CommonUtils.sleepMs(LOG, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerConf) * 3);
    Assert.assertFalse(mTFS.getFile(fileId1).isInMemory());
    Assert.assertTrue(mTFS.getFile(fileId3).isInMemory());
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
