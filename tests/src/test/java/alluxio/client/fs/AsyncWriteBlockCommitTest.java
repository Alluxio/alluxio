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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.FormatUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class AsyncWriteBlockCommitTest {

  private static final String TINY_WORKER_MEM = "512k";
  private static final String TINY_BLOCK_SIZE = "16k";

  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  public static LocalAlluxioClusterResource buildLocalAlluxioClusterResource() {
    LocalAlluxioClusterResource.Builder resource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, "8k")
        .setProperty(PropertyKey.USER_FILE_REPLICATION_DURABLE, 1)
        .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.WORKER_MEMORY_SIZE, TINY_WORKER_MEM)
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, TINY_BLOCK_SIZE);
    return resource.build();
  }

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      buildLocalAlluxioClusterResource();

  private static FileSystem sFileSystem;

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "25ms");
    mLocalAlluxioJobCluster.start();
    sFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() throws Exception {
    if (mLocalAlluxioJobCluster != null) {
      mLocalAlluxioJobCluster.stop();
    }
    sFileSystem.close();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME, "-1",
      PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "ASYNC_THROUGH",
      PropertyKey.Name.WORKER_MEMORY_SIZE, TINY_WORKER_MEM,
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, TINY_BLOCK_SIZE,
      PropertyKey.Name.USER_FILE_BUFFER_BYTES, TINY_BLOCK_SIZE,
      PropertyKey.Name.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, "10sec",
      "alluxio.worker.tieredstore.level0.watermark.high.ratio", "0.5",
      "alluxio.worker.tieredstore.level0.watermark.low.ratio", "0.25",
      })
  public void asyncWriteNoEvictBeforeBlockCommit() throws Exception {
    long writeSize =
        FormatUtils.parseSpaceSize(TINY_WORKER_MEM) - FormatUtils.parseSpaceSize(TINY_BLOCK_SIZE);
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    AlluxioURI p1 = new AlluxioURI("/p1");
    FileOutStream fos = fs.createFile(p1, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH)
        .setPersistenceWaitTime(-1).build());
    byte[] arr = new byte[(int) writeSize];
    Arrays.fill(arr, (byte) 0x7a);
    fos.write(arr);
    assertEquals(writeSize + FormatUtils.parseSpaceSize(TINY_BLOCK_SIZE),
        IntegrationTestUtils.getClusterCapacity(mLocalAlluxioClusterResource.get()));
    // subtract block size since the last block hasn't been committed yet
    assertEquals(writeSize, IntegrationTestUtils.getUsedWorkerSpace(mLocalAlluxioClusterResource));
    fos.close();
    FileSystemUtils.persistAndWait(fs, p1, 0);
    assertTrue(IntegrationTestUtils.getUsedWorkerSpace(mLocalAlluxioClusterResource) < writeSize);
  }
}
