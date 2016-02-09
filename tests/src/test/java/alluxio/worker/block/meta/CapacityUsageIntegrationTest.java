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

package alluxio.worker.block.meta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;

public class CapacityUsageIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 20 * Constants.MB;
  private static final int DISK_CAPACITY_BYTES = Constants.GB;
  private static final int USER_QUOTA_UNIT_BYTES = Constants.MB;
  private static final int HEARTBEAT_INTERVAL_MS = 30;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(MEM_CAPACITY_BYTES,
          MEM_CAPACITY_BYTES / 2, Constants.WORKER_TIERED_STORE_LEVELS, "2",
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 1), "HDD",
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 1), "/disk1",
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 1),
          String.valueOf(DISK_CAPACITY_BYTES), Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS,
          String.valueOf(HEARTBEAT_INTERVAL_MS));
  private FileSystem mFileSystem = null;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  private void createAndWriteFile(AlluxioURI filePath, WriteType writeType, int len)
      throws IOException, AlluxioException {
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < len; k++) {
      buf.put((byte) k);
    }

    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(writeType);
    FileOutStream os = mFileSystem.createFile(filePath, options);
    os.write(buf.array());
    os.close();
  }

  private void deleteDuringEviction(int i) throws IOException, AlluxioException {
    final AlluxioURI fileName1 = new AlluxioURI("/file" + i + "_1");
    final AlluxioURI fileName2 = new AlluxioURI("/file" + i + "_2");
    createAndWriteFile(fileName1, WriteType.CACHE_THROUGH, MEM_CAPACITY_BYTES);
    URIStatus fileStatus1 = mFileSystem.getStatus(fileName1);
    Assert.assertTrue(fileStatus1.getInMemoryPercentage() == 100);
    // Deleting file1, command will be sent by master to worker asynchronously
    mFileSystem.delete(fileName1);
    // Meanwhile creating file2. If creation arrives earlier than deletion, it will evict file1
    createAndWriteFile(fileName2, WriteType.CACHE_THROUGH, MEM_CAPACITY_BYTES / 4);
    URIStatus fileStatus2 = mFileSystem.getStatus(fileName2);
    Assert.assertTrue(fileStatus2.getInMemoryPercentage() == 100);
    mFileSystem.delete(fileName2);
  }

  // TODO(calvin): Rethink the approach of this test and what it should be testing.
  // @Test
  public void deleteDuringEvictionTest() throws IOException, AlluxioException {
    // This test may not trigger eviction each time, repeat it 20 times.
    for (int i = 0; i < 20; i++) {
      deleteDuringEviction(i);
      CommonUtils.sleepMs(2 * HEARTBEAT_INTERVAL_MS); // ensure second delete completes
    }
  }
}
