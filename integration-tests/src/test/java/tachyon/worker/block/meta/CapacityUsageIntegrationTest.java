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
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;

public class CapacityUsageIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 20 * Constants.MB;
  private static final int DISK_CAPACITY_BYTES = Constants.GB;
  private static final int USER_QUOTA_UNIT_BYTES = Constants.MB;
  private static final int HEARTBEAT_INTERVAL_MS = 30;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    // TODO Remove this once we are able to push tiered storage info to LocalTachyonCluster
    System.clearProperty(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL);
    System.clearProperty("tachyon.worker.tieredstore.level1.alias");
    System.clearProperty("tachyon.worker.tieredstore.level1.dirs.path");
    System.clearProperty("tachyon.worker.tieredstore.level1.dirs.quota");
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Before
  public final void before() throws IOException {
    // Disable hdfs client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    // TODO Need to change LocalTachyonCluster to pass this info to be set in TachyonConf
    System.setProperty(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, "2");
    System.setProperty("tachyon.worker.tieredstore.level1.alias", "HDD");
    System.setProperty("tachyon.worker.tieredstore.level1.dirs.path", "/disk1");
    System.setProperty("tachyon.worker.tieredstore.level1.dirs.quota", DISK_CAPACITY_BYTES + "");

    mLocalTachyonCluster =
        new LocalTachyonCluster(MEM_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, MEM_CAPACITY_BYTES / 2);
    mLocalTachyonCluster.start();

    mLocalTachyonCluster.getWorkerTachyonConf().set(
        Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS + "");
    mTFS = mLocalTachyonCluster.getClient();
  }

  private int createAndWriteFile(TachyonURI filePath, WriteType writeType, int len)
      throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < len; k ++) {
      buf.put((byte) k);
    }
    int fileId = mTFS.createFile(filePath);
    TachyonFile file = mTFS.getFile(fileId);
    OutStream os = file.getOutStream(writeType);
    os.write(buf.array());
    os.close();
    return fileId;
  }

  private void deleteDuringEviction(int i) throws IOException {
    final String fileName1 = "/file" + i + "_1";
    final String fileName2 = "/file" + i + "_2";
    int fileId =
        createAndWriteFile(new TachyonURI(fileName1), WriteType.CACHE_THROUGH, MEM_CAPACITY_BYTES);
    TachyonFile file = mTFS.getFile(fileId);
    Assert.assertTrue(file.isInMemory());
    // Deleting file1, command will be sent by master to worker asynchronously
    mTFS.delete(new TachyonURI(fileName1), false);
    // Meanwhile creating file2. If creation arrives earlier than deletion, it will evict file1
    fileId =
        createAndWriteFile(new TachyonURI(fileName2), WriteType.CACHE_THROUGH,
            MEM_CAPACITY_BYTES / 4);
    file = mTFS.getFile(fileId);
    Assert.assertTrue(file.isInMemory());
    mTFS.delete(new TachyonURI(fileName2), false);
  }

  @Test
  public void deleteDuringEvictionTest() throws IOException {
    // This test may not trigger eviction each time, repeat it 20 times.
    for (int i = 0; i < 20; i ++) {
      deleteDuringEviction(i);
      CommonUtils.sleepMs(null, 2 * HEARTBEAT_INTERVAL_MS); // ensure second delete completes
    }
  }
}
