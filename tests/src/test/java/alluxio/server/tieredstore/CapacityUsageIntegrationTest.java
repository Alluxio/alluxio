/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.server.tieredstore;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CapacityUsageIntegrationTest extends BaseIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 20 * Constants.MB;
  private static final int DISK_CAPACITY_BYTES = Constants.GB;
  private static final int HEARTBEAT_INTERVAL_MS = 30;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, MEM_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, MEM_CAPACITY_BYTES / 2)
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2")
          .setProperty(
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1), Constants.MEDIUM_HDD)
          .setProperty(
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1),
              "/disk1")
          .setProperty(
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1),
              String.valueOf(DISK_CAPACITY_BYTES))
          .setProperty(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS,
              String.valueOf(HEARTBEAT_INTERVAL_MS))
          .build();
  private FileSystem mFileSystem = null;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  private void createAndWriteFile(AlluxioURI filePath, WritePType writeType, int len)
      throws IOException, AlluxioException {
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < len; k++) {
      buf.put((byte) k);
    }

    CreateFilePOptions options = CreateFilePOptions.newBuilder().setWriteType(writeType).build();
    FileOutStream os = mFileSystem.createFile(filePath, options);
    os.write(buf.array());
    os.close();
  }

  private void deleteDuringEviction(int i) throws IOException, AlluxioException {
    final AlluxioURI fileName1 = new AlluxioURI("/file" + i + "_1");
    final AlluxioURI fileName2 = new AlluxioURI("/file" + i + "_2");
    createAndWriteFile(fileName1, WritePType.CACHE_THROUGH, MEM_CAPACITY_BYTES);
    URIStatus fileStatus1 = mFileSystem.getStatus(fileName1);
    Assert.assertTrue(fileStatus1.getInMemoryPercentage() == 100);
    // Deleting file1, command will be sent by master to worker asynchronously
    mFileSystem.delete(fileName1);
    // Meanwhile creating file2. If creation arrives earlier than deletion, it will evict file1
    createAndWriteFile(fileName2, WritePType.CACHE_THROUGH, MEM_CAPACITY_BYTES / 4);
    URIStatus fileStatus2 = mFileSystem.getStatus(fileName2);
    Assert.assertTrue(fileStatus2.getInMemoryPercentage() == 100);
    mFileSystem.delete(fileName2);
  }

  // TODO(calvin): Rethink the approach of this test and what it should be testing.
  // @Test
  public void deleteDuringEviction() throws IOException, AlluxioException {
    // This test may not trigger eviction each time, repeat it 20 times.
    for (int i = 0; i < 20; i++) {
      deleteDuringEviction(i);
      CommonUtils.sleepMs(2 * HEARTBEAT_INTERVAL_MS); // ensure second delete completes
    }
  }
}
