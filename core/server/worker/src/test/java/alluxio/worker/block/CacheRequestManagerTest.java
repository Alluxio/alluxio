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

package alluxio.worker.block;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.Sessions;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CacheRequest;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.grpc.GrpcExecutors;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Unit tests for {@link CacheRequestManager}.
 */
public class CacheRequestManagerTest {
  private CacheRequestManager mCacheRequestManager;
  private Random mRandom;
  private BlockWorker mBlockWorker;
  private FileSystemContext mFsContext;

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws IOException {
    mRandom = new Random();
    mBlockWorker = mock(DefaultBlockWorker.class);
    mFsContext = mock(FileSystemContext.class);
    mCacheRequestManager =
        new CacheRequestManager(GrpcExecutors.CACHE_MANAGER_EXECUTOR, mBlockWorker, mFsContext);
  }

  @Test
  public void submitRequestCacheBlockFromUfs() throws Exception {
    String localWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    int port = mRandom.nextInt();
    long blockId = mRandom.nextLong();
    long blockLength = mRandom.nextLong();
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();
    CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(openUfsBlockOptions).setSourceHost(localWorkerHostname)
        .setSourcePort(port).build();
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker).createBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, 0, "",
        blockLength);
    verify(mBlockWorker).createUfsBlockReader(Sessions.ASYNC_CACHE_UFS_SESSION_ID, blockId, 0,
        false, openUfsBlockOptions);
  }

  @Test
  public void submitRequestCacheBlockFromRemoteWorker() throws Exception {
    String remoteWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS) + 1);
    int port = mRandom.nextInt();
    long blockId = mRandom.nextLong();
    long blockLength = mRandom.nextLong();
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();
    CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(openUfsBlockOptions).setSourceHost(remoteWorkerHostname)
        .setSourcePort(port).build();
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker).createBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, 0, "",
        blockLength);
    verify(mBlockWorker).createBlockWriter(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId);
    verify(mBlockWorker).commitBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, false);
  }
}
