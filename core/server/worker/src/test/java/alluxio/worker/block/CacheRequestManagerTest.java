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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.Sessions;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.grpc.CacheRequest;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.grpc.GrpcExecutors;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
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
    mCacheRequestManager = spy(new CacheRequestManager(GrpcExecutors.ASYNC_CACHE_MANAGER_EXECUTOR,
        mBlockWorker, mFsContext));
  }

  @Test
  public void submitRequestCacheBlockFromUfs() throws Exception {
    String localWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    int port = mRandom.nextInt(5000);
    long blockId = mRandom.nextLong();
    long blockLength = mRandom.nextInt(5000);
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();
    CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(openUfsBlockOptions).setSourceHost(localWorkerHostname)
        .setSourcePort(port).build();
    BlockReader mockBlockReader = mock(BlockReader.class);
    when(mBlockWorker.createUfsBlockReader(Sessions.ASYNC_CACHE_UFS_SESSION_ID, blockId, 0, false,
        openUfsBlockOptions)).thenReturn(mockBlockReader);
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker).createUfsBlockReader(Sessions.ASYNC_CACHE_UFS_SESSION_ID, blockId, 0,
        false, openUfsBlockOptions);
  }

  @Test
  public void submitRequestCacheBlockFromRemoteWorker() throws Exception {
    String localWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    String fakeRemoteWorker = localWorkerHostname + "1";
    int port = mRandom.nextInt(5000);
    long blockId = mRandom.nextLong();
    long blockLength = mRandom.nextInt(5000);
    Protocol.OpenUfsBlockOptions options = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();
    CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(options).setSourceHost(fakeRemoteWorker)
        .setSourcePort(port).build();
    setupMockReaderWriter(fakeRemoteWorker, port, blockId, blockLength, options);
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker).createBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, 0, "",
        blockLength);
    verify(mBlockWorker).createBlockWriter(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId);
    verify(mBlockWorker).commitBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, false);
  }

  @Test
  public void submitAsyncRequestCacheBlockFromUfs() throws Exception {
    String localWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    int port = mRandom.nextInt(5000);
    long blockId = mRandom.nextLong();
    long blockLength = mRandom.nextInt(5000);
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();
    CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(openUfsBlockOptions).setSourceHost(localWorkerHostname)
        .setSourcePort(port).setAsync(true).build();
    BlockReader mockBlockReader = mock(BlockReader.class);
    when(mBlockWorker.createUfsBlockReader(Sessions.ASYNC_CACHE_UFS_SESSION_ID, blockId, 0, false,
        openUfsBlockOptions)).thenReturn(mockBlockReader);
    mCacheRequestManager.submitRequest(request);
    Thread.sleep(100);  //sleep since it's async
    verify(mBlockWorker).createUfsBlockReader(Sessions.ASYNC_CACHE_UFS_SESSION_ID, blockId, 0,
        false, openUfsBlockOptions);
  }

  @Test
  public void submitAsyncRequestCacheBlockFromRemoteWorker() throws Exception {
    String localWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    String fakeRemoteWorker = localWorkerHostname + "1";
    int port = mRandom.nextInt(5000);
    long blockId = mRandom.nextLong();
    long blockLength = mRandom.nextInt(5000);
    Protocol.OpenUfsBlockOptions options = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();
    CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(options).setSourceHost(fakeRemoteWorker)
        .setSourcePort(port).setAsync(true).build();
    setupMockReaderWriter(fakeRemoteWorker, port, blockId, blockLength, options);
    mCacheRequestManager.submitRequest(request);
    Thread.sleep(100); //sleep since it's async
    verify(mBlockWorker).createBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, 0, "",
        blockLength);
    verify(mBlockWorker).createBlockWriter(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId);
    verify(mBlockWorker).commitBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, false);
  }

  private void setupMockReaderWriter(String source, int port, long blockId, long blockLength,
      Protocol.OpenUfsBlockOptions options)
      throws IOException, BlockDoesNotExistException, BlockAlreadyExistsException,
      InvalidWorkerStateException {
    InetSocketAddress sourceAddress =
        new InetSocketAddress(source, port);
    RemoteBlockReader reader = mock(RemoteBlockReader.class);
    when(mCacheRequestManager.getRemoteBlockReader(blockId, blockLength, sourceAddress, options))
        .thenReturn(reader);
    ReadableByteChannel readerChannel = mock(ReadableByteChannel.class);
    when(reader.getChannel()).thenReturn(readerChannel);
    int bufferSize = 4 * Constants.MB;
    ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
    when(readerChannel.read(buffer)).thenReturn(-1);
    BlockWriter writer = mock(BlockWriter.class);
    when(mBlockWorker.createBlockWriter(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId))
        .thenReturn(writer);
    WritableByteChannel writerChannel = mock(WritableByteChannel.class);
    when(writer.getChannel()).thenReturn(writerChannel);
  }
}
