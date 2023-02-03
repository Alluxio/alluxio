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

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStoreType;
import alluxio.worker.block.BlockWorker;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class PagedBlockStoreIntegrationTest extends BaseIntegrationTest {
  private final String mUfsPath =
      AlluxioTestDirectory.createTemporaryDirectory("ufs").getAbsolutePath();

  @Rule
  public LocalAlluxioClusterResource mLocalCluster = new LocalAlluxioClusterResource.Builder()
      // page store in worker does not support short circuit
      .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
      .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, BlockStoreType.PAGE)
      .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, ImmutableList.of(Constants.MB))
      .setProperty(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(
          AlluxioTestDirectory.createTemporaryDirectory("page_store").getAbsolutePath()))
      .setProperty(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, Constants.KB)
      .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsPath)
      .build();

  /**
   * Tests that the pages stored in the local page store are preserved across worker restarts.
   */
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_PAGE_STORE_TYPE, "LOCAL",
  })
  @Test
  public void testLocalPageStorePreservesPagesAfterRestartWithGrpc() throws Exception {
    testLocalPageStorePreservesPagesAfterRestart();
  }

  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_PAGE_STORE_TYPE, "LOCAL",
      PropertyKey.Name.USER_NETTY_DATA_TRANSMISSION_ENABLED, "true",
      PropertyKey.Name.WORKER_NETWORK_NETTY_CHANNEL, "nio",
      PropertyKey.Name.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, "TRANSFER"
  })
  @Test
  public void testLocalPageStorePreservesPagesAfterRestartWithNetty() throws Exception {
    testLocalPageStorePreservesPagesAfterRestart();
  }

  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.WORKER_PAGE_STORE_PAGE_SIZE, "64",
      PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "16",
      PropertyKey.Name.WORKER_NETWORK_READER_BUFFER_POOLED, "false"
  })
  @Test
  public void testReadUnpooledWithGrpc() throws Exception {
    testRead();
  }

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
      PropertyKey.Name.WORKER_NETWORK_READER_BUFFER_POOLED, "true"
  })
  @Test
  public void testReadPooledWithGrpc() throws Exception {
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

  private void testLocalPageStorePreservesPagesAfterRestart() throws Exception {
    // prepare data in UFS
    try (UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot(Configuration.global());
         OutputStream os = ufs.create(PathUtils.concatPath(mUfsPath, "read-from-ufs"))) {
      os.write(BufferUtils.getIncreasingByteArray(Constants.KB * 2));
    }
    // create a file through alluxio
    final int startOffset = 1;
    LocalAlluxioMaster master = mLocalCluster.get().getLocalAlluxioMaster();
    try (OutputStream os = master.getClient().createFile(
        new AlluxioURI("/write-into-alluxio"),
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build())) {
      os.write(BufferUtils.getIncreasingByteArray(startOffset, Constants.KB));
    }
    // read the file from UFS so that it gets cached in worker storage
    try (InputStream is = master.getClient().openFile(
        new AlluxioURI("/read-from-ufs"),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
      byte[] content = ByteStreams.toByteArray(is);
      assertTrue(BufferUtils.equalIncreasingByteArray(Constants.KB * 2, content));
    }
    try (InputStream is = master.getClient().openFile(
        new AlluxioURI("/write-into-alluxio"),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
      byte[] content = ByteStreams.toByteArray(is);
      assertTrue(BufferUtils.equalIncreasingByteArray(startOffset, Constants.KB, content));
    }
    // check the blocks are in the worker
    BlockWorker worker =
        mLocalCluster.get().getWorkerProcess().getWorker(BlockWorker.class);
    for (String path : ImmutableList.of("/read-from-ufs", "/write-into-alluxio")) {
      URIStatus status = master.getClient().getStatus(new AlluxioURI(path));
      List<Long> blocks = status.getBlockIds();
      for (long block : blocks) {
        assertTrue(worker.getBlockStore().hasBlockMeta(block));
      }
    }
    // restart the worker
    mLocalCluster.get().stopWorkers();
    mLocalCluster.get().startWorkers();
    mLocalCluster.get().waitForWorkersRegistered(5000);
    // verify the blocks are still there
    worker = mLocalCluster.get().getWorkerProcess().getWorker(BlockWorker.class);
    for (String path : ImmutableList.of("/read-from-ufs", "/write-into-alluxio")) {
      URIStatus status = master.getClient().getStatus(new AlluxioURI(path));
      List<Long> blocks = status.getBlockIds();
      for (long block : blocks) {
        assertTrue(worker.getBlockStore().hasBlockMeta(block));
      }
    }
  }
}
