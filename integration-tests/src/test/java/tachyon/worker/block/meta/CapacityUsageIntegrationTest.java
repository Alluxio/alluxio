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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.CacheType;
import tachyon.client.ClientOptions;
import tachyon.client.OutStream;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;

public class CapacityUsageIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 20 * Constants.MB;
  private static final int DISK_CAPACITY_BYTES = Constants.GB;
  private static final int USER_QUOTA_UNIT_BYTES = Constants.MB;
  private static final int HEARTBEAT_INTERVAL_MS = 30;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTFS = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    // TODO Remove this once we are able to push tiered storage info to LocalTachyonCluster
    System.clearProperty(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL);
    System.clearProperty(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 1));
    System.clearProperty(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 1));
    System.clearProperty(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 1));
  }

  @Before
  public final void before() throws Exception {
    // TODO Need to change LocalTachyonCluster to pass this info to be set in TachyonConf
    System.setProperty(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, "2");
    System.setProperty(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 1), "HDD");
    System.setProperty(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 1),
        "/disk1");
    System.setProperty(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 1),
        DISK_CAPACITY_BYTES + "");

    mLocalTachyonCluster =
        new LocalTachyonCluster(MEM_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, MEM_CAPACITY_BYTES / 2);
    mLocalTachyonCluster.start();

    mLocalTachyonCluster.getWorkerTachyonConf()
        .set(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS + "");
    mTFS = mLocalTachyonCluster.getClient();
  }

  private TachyonFile createAndWriteFile(TachyonURI filePath, CacheType cacheType,
      UnderStorageType underStorageType, int len) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < len; k ++) {
      buf.put((byte) k);
    }

    ClientOptions options = new ClientOptions.Builder(new TachyonConf()).setCacheType(cacheType)
        .setUnderStorageType(underStorageType).build();
    OutStream os = mTFS.getOutStream(filePath, options);
    os.write(buf.array());
    os.close();
    return mTFS.open(filePath);
  }

  private void deleteDuringEviction(int i) throws IOException {
    final String fileName1 = "/file" + i + "_1";
    final String fileName2 = "/file" + i + "_2";
    TachyonFile file1 = createAndWriteFile(new TachyonURI(fileName1), CacheType.CACHE,
        UnderStorageType.PERSIST, MEM_CAPACITY_BYTES);
    FileInfo fileInfo1 = mTFS.getInfo(file1);
    Assert.assertTrue(fileInfo1.getInMemoryPercentage() == 100);
    // Deleting file1, command will be sent by master to worker asynchronously
    mTFS.delete(file1);
    // Meanwhile creating file2. If creation arrives earlier than deletion, it will evict file1
    TachyonFile file2 = createAndWriteFile(new TachyonURI(fileName2), CacheType.CACHE,
        UnderStorageType.PERSIST, MEM_CAPACITY_BYTES / 4);
    FileInfo fileInfo2 = mTFS.getInfo(file2);
    Assert.assertTrue(fileInfo2.getInMemoryPercentage() == 100);
    mTFS.delete(file2);
  }

  // TODO: Rethink the approach of this test and what it should be testing
  // @Test
  public void deleteDuringEvictionTest() throws IOException {
    // This test may not trigger eviction each time, repeat it 20 times.
    for (int i = 0; i < 20; i ++) {
      deleteDuringEviction(i);
      CommonUtils.sleepMs(2 * HEARTBEAT_INTERVAL_MS); // ensure second delete completes
    }
  }
}
