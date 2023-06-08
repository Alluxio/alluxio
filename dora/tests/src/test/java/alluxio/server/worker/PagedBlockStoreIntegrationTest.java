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

package alluxio.server.worker;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockStoreType;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class PagedBlockStoreIntegrationTest extends BaseIntegrationTest {
  private final String mUfsPath =
      AlluxioTestDirectory.createTemporaryDirectory("ufs").getAbsolutePath();

  @Rule
  public LocalAlluxioClusterResource mLocalCluster = new LocalAlluxioClusterResource.Builder()
      // page store in worker does not support short circuit
      .setProperty(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED, true)
      .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
      .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, mUfsPath)
      .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
      .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, BlockStoreType.PAGE)
      .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, ImmutableList.of(Constants.MB))
      .setProperty(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(
          AlluxioTestDirectory.createTemporaryDirectory("page_store").getAbsolutePath()))
      .setProperty(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, Constants.KB)
      .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsPath)
      .build();

  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_PAGE_STORE_PAGE_SIZE, "64",
      PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "16",
      PropertyKey.Name.WORKER_NETWORK_READER_BUFFER_POOLED, "false",
      PropertyKey.Name.USER_NETTY_DATA_TRANSMISSION_ENABLED, "true",
      PropertyKey.Name.WORKER_NETWORK_NETTY_CHANNEL, "nio",
      PropertyKey.Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, "TRANSFER"
  })
  @Test
  public void testReadUnpooledWithNetty() throws Exception {
    testRead();
  }

  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_PAGE_STORE_PAGE_SIZE, "64",
      PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "16",
      PropertyKey.Name.WORKER_NETWORK_READER_BUFFER_POOLED, "true",
      PropertyKey.Name.USER_NETTY_DATA_TRANSMISSION_ENABLED, "true",
      PropertyKey.Name.WORKER_NETWORK_NETTY_CHANNEL, "nio",
      PropertyKey.Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, "TRANSFER"
  })
  @Test
  public void testReadPooledWithNetty() throws Exception {
    testRead();
  }

  private void testRead() throws Exception {
    final int fileSize = 1024;
    FileSystem client = mLocalCluster.get().getLocalAlluxioMaster().getClient();
    try (FileOutStream os = client.createFile(new AlluxioURI("/test"))) {
      os.write(BufferUtils.getIncreasingByteArray((fileSize)));
    }
    try (FileInStream is = client.openFile(new AlluxioURI("/test"),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
      byte[] content = ByteStreams.toByteArray(is);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileSize, content));
    }
  }
}
