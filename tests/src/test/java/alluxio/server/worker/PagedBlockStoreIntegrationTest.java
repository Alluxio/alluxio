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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.PropertyKey;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.BlockStoreType;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class PagedBlockStoreIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mCluster = new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
      .setProperty(PropertyKey.USER_BLOCK_STORE_TYPE, BlockStoreType.PAGE)
      .setProperty(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.LOCAL)
      .setProperty(PropertyKey.USER_CLIENT_CACHE_DIRS, ImmutableList.of(
          AlluxioTestDirectory.createTemporaryDirectory("page_store").getAbsolutePath()))
      .setProperty(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, 64)
      .setProperty(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, 16)
      .build();

  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_NETWORK_READER_BUFFER_POOLED, "false"
  })
  @Test
  public void testUnpooled() throws Exception {
    test();
  }

  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_NETWORK_READER_BUFFER_POOLED, "true"
  })
  @Test
  public void testPooled() throws Exception {
    test();
  }

  private void test() throws Exception {
    final int fileSize = 256;
    FileSystem client = mCluster.get().getLocalAlluxioMaster().getClient();
    try (FileOutStream os = client.createFile(new AlluxioURI("/test"))) {
      os.write(BufferUtils.getIncreasingByteArray((fileSize)));
    }
    try (FileInStream is = client.openFile(new AlluxioURI("/test"),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
      byte[] content = ByteStreams.toByteArray(is);
      System.out.println(Arrays.toString(content));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileSize, content));
    }
  }
}
