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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;

public class CapacityUsageIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 20 * Constants.MB;
  private static final int DISK_CAPACITY_BYTES = Constants.GB;
  private static final int USER_QUOTA_UNIT_BYTES = Constants.MB;
  private static final int HEARTBEAT_INTERVAL_MS = 30;

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(MEM_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES,
          MEM_CAPACITY_BYTES / 2, Constants.WORKER_TIERED_STORE_LEVELS, "2",
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 1), "HDD",
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 1), "/disk1",
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 1),
          String.valueOf(DISK_CAPACITY_BYTES), Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS,
          String.valueOf(HEARTBEAT_INTERVAL_MS));
  private TachyonFileSystem mTFS = null;

  @Before
  public final void before() throws Exception {
    mTFS = mLocalTachyonClusterResource.get().getClient();
  }

  private TachyonFile createAndWriteFile(TachyonURI filePath, TachyonStorageType tachyonStorageType,
      UnderStorageType underStorageType, int len) throws IOException, TachyonException {
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < len; k ++) {
      buf.put((byte) k);
    }

    OutStreamOptions options = new OutStreamOptions.Builder(new TachyonConf())
        .setTachyonStorageType(tachyonStorageType).setUnderStorageType(underStorageType).build();
    FileOutStream os = mTFS.getOutStream(filePath, options);
    os.write(buf.array());
    os.close();
    return mTFS.open(filePath);
  }

  private void deleteDuringEviction(int i) throws IOException, TachyonException {
    final String fileName1 = "/file" + i + "_1";
    final String fileName2 = "/file" + i + "_2";
    TachyonFile file1 = createAndWriteFile(new TachyonURI(fileName1), TachyonStorageType.STORE,
        UnderStorageType.SYNC_PERSIST, MEM_CAPACITY_BYTES);
    FileInfo fileInfo1 = mTFS.getInfo(file1);
    Assert.assertTrue(fileInfo1.getInMemoryPercentage() == 100);
    // Deleting file1, command will be sent by master to worker asynchronously
    mTFS.delete(file1);
    // Meanwhile creating file2. If creation arrives earlier than deletion, it will evict file1
    TachyonFile file2 = createAndWriteFile(new TachyonURI(fileName2), TachyonStorageType.STORE,
        UnderStorageType.SYNC_PERSIST, MEM_CAPACITY_BYTES / 4);
    FileInfo fileInfo2 = mTFS.getInfo(file2);
    Assert.assertTrue(fileInfo2.getInMemoryPercentage() == 100);
    mTFS.delete(file2);
  }

  // TODO(calvin): Rethink the approach of this test and what it should be testing.
  // @Test
  public void deleteDuringEvictionTest() throws IOException, TachyonException {
    // This test may not trigger eviction each time, repeat it 20 times.
    for (int i = 0; i < 20; i ++) {
      deleteDuringEviction(i);
      CommonUtils.sleepMs(2 * HEARTBEAT_INTERVAL_MS); // ensure second delete completes
    }
  }
}
