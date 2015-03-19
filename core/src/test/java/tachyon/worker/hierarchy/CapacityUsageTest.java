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

package tachyon.worker.hierarchy;

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
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.util.CommonUtils;

public class CapacityUsageTest {
  private static final int MEM_CAPACITY_BYTES = 20 * Constants.MB;
  private static final int DISK_CAPACITY_BYTES = Constants.GB;
  private static final int USER_QUOTA_UNIT_BYTES = Constants.MB;
  private static final int HEARTBEAT_INTERVAL_MS = 30;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty(Constants.WORKER_MAX_HIERARCHY_STORAGE_LEVEL);
  }

  @Before
  public final void before() throws IOException {
    System.setProperty(Constants.WORKER_MAX_HIERARCHY_STORAGE_LEVEL, "2");
    System.setProperty("tachyon.worker.hierarchystore.level1.alias", "HDD");
    System.setProperty("tachyon.worker.hierarchystore.level1.dirs.path", "/disk1");
    System
        .setProperty("tachyon.worker.hierarchystore.level1.dirs.quota", DISK_CAPACITY_BYTES + "");

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

  private void DeleteDuringEviction() throws IOException {
    int fileId =
        createAndWriteFile(new TachyonURI("/file1"), WriteType.CACHE_THROUGH, MEM_CAPACITY_BYTES);
    TachyonFile file = mTFS.getFile(fileId);
    Assert.assertEquals(true, file.isInMemory());
    mTFS.delete(new TachyonURI("/file1"), false);
    fileId =
        createAndWriteFile(new TachyonURI("/file1"), WriteType.CACHE_THROUGH,
            MEM_CAPACITY_BYTES / 4);
    file = mTFS.getFile(fileId);
    Assert.assertEquals(true, file.isInMemory());
    mTFS.delete(new TachyonURI("/file1"), false);
  }

  @Test
  public void DeleteDuringEvictionTest() throws IOException {
    for (int i = 5; i > 0; i --) {
      DeleteDuringEviction();
      CommonUtils.sleepMs(null, HEARTBEAT_INTERVAL_MS + HEARTBEAT_INTERVAL_MS / i);
    }
  }
}
