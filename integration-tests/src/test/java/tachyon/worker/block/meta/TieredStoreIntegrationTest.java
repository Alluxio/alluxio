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
import tachyon.client.TachyonStorageType;
import tachyon.client.ClientOptions;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;
import tachyon.util.io.BufferUtils;

/**
 * Integration tests for {@link tachyon.worker.block.meta.StorageTier}.
 */
public class TieredStoreIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int MEM_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster;
  private TachyonConf mWorkerConf;
  private TachyonFileSystem mTFS;
  private int mWorkerToMasterHeartbeatIntervalMs;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(100));

    mLocalTachyonCluster =
        new LocalTachyonCluster(MEM_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB);
    mLocalTachyonCluster.start(tachyonConf);
    mTFS = mLocalTachyonCluster.getClient();
    mWorkerConf = mLocalTachyonCluster.getWorkerTachyonConf();
    mWorkerToMasterHeartbeatIntervalMs =
        mWorkerConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
  }

  // Tests that deletes go through despite failing initially due to concurrent read
  @Test
  public void deleteWhileReadTest() throws Exception {
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTFS, "/test1", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, MEM_CAPACITY_BYTES);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    Assert.assertTrue(mTFS.getInfo(file).getInMemoryPercentage() == 100);
    // Open the file
    ClientOptions options =
        new ClientOptions.Builder(new TachyonConf()).setTachyonStoreType(TachyonStorageType.STORE)
            .build();
    FileInStream in = mTFS.getInStream(file, options);
    Assert.assertEquals(0, in.read());

    // Delete the file
    mTFS.delete(file);

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the delete, the master should no longer serve the file
    try {
      mTFS.open(new TachyonURI("/test1"));
      Assert.fail("file should not exist: /test1");
    } catch (IOException ioe) {
      // This is expected, since the file should not exist.
    }

    // However, the previous read should still be able to read it as the data still exists
    byte[] res = new byte[MEM_CAPACITY_BYTES];
    Assert.assertEquals(MEM_CAPACITY_BYTES - 1, in.read(res, 1, MEM_CAPACITY_BYTES - 1));
    res[0] = 0;
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(MEM_CAPACITY_BYTES, res));
    in.close();

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // After the file is closed, the master's delete should go through and new files can be created
    TachyonFile newFile =
        TachyonFSTestUtils.createByteFile(mTFS, "/test2", TachyonStorageType.STORE,
            UnderStorageType.NO_PERSIST, MEM_CAPACITY_BYTES);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);
    Assert.assertTrue(mTFS.getInfo(newFile).getInMemoryPercentage() == 100);
  }

  // Tests that pinning a file prevents it from being evicted.
  @Test
  public void pinFileTest() throws Exception {
    // Create a file that fills the entire Tachyon store
    TachyonFile file =
        TachyonFSTestUtils.createByteFile(mTFS, "/test1", TachyonStorageType.STORE,
            UnderStorageType.NO_PERSIST, MEM_CAPACITY_BYTES);

    // Pin the file
    mTFS.setPin(file, true);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the pin with master
    Assert.assertTrue(mTFS.getInfo(file).isIsPinned());

    // Try to create a file that cannot be stored unless the previous file is evicted, expect an
    // exception since worker cannot serve the request
    mThrown.expect(IOException.class);
    TachyonFSTestUtils.createByteFile(mTFS, "/test2", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, MEM_CAPACITY_BYTES);
  }

  // Tests that pinning a file and then unpinning
  @Test
  public void unpinFileTest() throws Exception {
    // Create a file that fills the entire Tachyon store
    TachyonFile file1 = TachyonFSTestUtils.createByteFile(mTFS, "/test1", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, MEM_CAPACITY_BYTES);

    // Pin the file
    mTFS.setPin(file1, true);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the pin with master
    Assert.assertTrue(mTFS.getInfo(file1).isIsPinned());

    // Unpin the file
    mTFS.setPin(file1, false);
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    // Confirm the unpin
    Assert.assertFalse(mTFS.getInfo(file1).isIsPinned());

    // Try to create a file that cannot be stored unless the previous file is evicted, this
    // should succeed
    TachyonFile file2 = TachyonFSTestUtils.createByteFile(mTFS, "/test2", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, MEM_CAPACITY_BYTES);

    // File 2 should be in memory and File 1 should be evicted
    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);
    Assert.assertFalse(mTFS.getInfo(file1).getInMemoryPercentage() == 100);
    Assert.assertTrue(mTFS.getInfo(file2).getInMemoryPercentage() == 100);
  }

  @Test
  public void promoteBlock() throws IOException, InterruptedException {
    TachyonFile file1 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test1", TachyonStorageType.STORE,
            UnderStorageType.PERSIST, MEM_CAPACITY_BYTES / 6);
    TachyonFile file2 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test2", TachyonStorageType.STORE,
            UnderStorageType.PERSIST, MEM_CAPACITY_BYTES / 2);
    TachyonFile file3 =
        TachyonFSTestUtils.createByteFile(mTFS, "/root/test3", TachyonStorageType.STORE,
            UnderStorageType.PERSIST, MEM_CAPACITY_BYTES / 2);

    CommonUtils.sleepMs(null, mWorkerToMasterHeartbeatIntervalMs * 3);

    TachyonFile toPromote = null;
    FileInfo file1Info = mTFS.getInfo(file1);
    FileInfo file2Info = mTFS.getInfo(file2);
    FileInfo file3Info = mTFS.getInfo(file3);

    if (file1Info.getInMemoryPercentage() < 100) {
      toPromote = file1;
      Assert.assertEquals(100, file2Info.getInMemoryPercentage());
      Assert.assertEquals(100, file3Info.getInMemoryPercentage());
    } else if (file2Info.getInMemoryPercentage() < 100) {
      toPromote = file2;
      Assert.assertEquals(100, file1Info.getInMemoryPercentage());
      Assert.assertEquals(100, file3Info.getInMemoryPercentage());
    } else {
      toPromote = file3;
      Assert.assertEquals(100, file1Info.getInMemoryPercentage());
      Assert.assertEquals(100, file2Info.getInMemoryPercentage());
    }

    FileInStream is = mTFS.getInStream(toPromote, new ClientOptions.Builder(mWorkerConf)
        .setTachyonStoreType(TachyonStorageType.PROMOTE).build());
    byte[] buf = new byte[MEM_CAPACITY_BYTES / 6];
    int len = is.read(buf);
    is.close();

    CommonUtils.sleepMs(LOG, mWorkerToMasterHeartbeatIntervalMs * 3);

    Assert.assertEquals(MEM_CAPACITY_BYTES / 6, len);
    Assert.assertEquals(100, mTFS.getInfo(toPromote).getInMemoryPercentage());
  }

}
