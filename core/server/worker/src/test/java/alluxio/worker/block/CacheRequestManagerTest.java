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
import static org.mockito.Mockito.timeout;
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

/**
 * Unit tests for {@link CacheRequestManager}.
 */
public class CacheRequestManagerTest {
  private static final long BLOCK_ID = 2L;
  private static final int CHUNK_SIZE = 128;
  private static final int PORT = 22;

  private CacheRequestManager mCacheRequestManager;
  private BlockWorker mBlockWorker;
  private FileSystemContext mFsContext;
  private String mLocalWorkerHostname;
  private Protocol.OpenUfsBlockOptions mOptions;


  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws IOException {
    mBlockWorker = mock(DefaultBlockWorker.class);
    mFsContext = mock(FileSystemContext.class);
    mCacheRequestManager = spy(new CacheRequestManager(GrpcExecutors.CACHE_MANAGER_EXECUTOR,
        mBlockWorker, mFsContext));
    mLocalWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    mOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();
  }

  @Test
  public void submitRequestCacheBlockFromUfs() throws Exception {

    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOptions).setSourceHost(mLocalWorkerHostname)
        .setSourcePort(PORT).build();
    BlockReader mockBlockReader = mock(BlockReader.class);
    when(mBlockWorker.createUfsBlockReader(Sessions.CACHE_UFS_SESSION_ID, BLOCK_ID, 0, false,
        mOptions)).thenReturn(mockBlockReader);
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker).createUfsBlockReader(Sessions.CACHE_UFS_SESSION_ID, BLOCK_ID, 0,
        false, mOptions);
  }

  @Test
  public void submitRequestCacheBlockFromRemoteWorker() throws Exception {
    String fakeRemoteWorker = mLocalWorkerHostname + "1";
    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOptions).setSourceHost(fakeRemoteWorker).setSourcePort(PORT)
        .build();
    setupMockReaderWriter(fakeRemoteWorker, PORT, BLOCK_ID, CHUNK_SIZE, mOptions);
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker).createBlock(Sessions.CACHE_WORKER_SESSION_ID, BLOCK_ID, 0, "", CHUNK_SIZE);
    verify(mBlockWorker).createBlockWriter(Sessions.CACHE_WORKER_SESSION_ID, BLOCK_ID);
    verify(mBlockWorker).commitBlock(Sessions.CACHE_WORKER_SESSION_ID, BLOCK_ID, false);
  }

  @Test
  public void submitAsyncRequestCacheBlockFromUfs() throws Exception {
    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOptions).setSourceHost(mLocalWorkerHostname)
        .setSourcePort(PORT).setAsync(true).build();
    BlockReader mockBlockReader = mock(BlockReader.class);
    when(mBlockWorker.createUfsBlockReader(Sessions.CACHE_UFS_SESSION_ID, BLOCK_ID, 0, false,
        mOptions)).thenReturn(mockBlockReader);
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker, timeout(30000)).createUfsBlockReader(Sessions.CACHE_UFS_SESSION_ID,
        BLOCK_ID, 0, false, mOptions);
  }

  @Test
  public void submitAsyncRequestCacheBlockFromRemoteWorker() throws Exception {

    String fakeRemoteWorker = mLocalWorkerHostname + "1";

    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOptions).setSourceHost(fakeRemoteWorker).setSourcePort(PORT)
        .setAsync(true).build();
    setupMockReaderWriter(fakeRemoteWorker, PORT, BLOCK_ID, CHUNK_SIZE, mOptions);
    mCacheRequestManager.submitRequest(request);
    verify(mBlockWorker, timeout(30000)).createBlock(Sessions.CACHE_WORKER_SESSION_ID,
        BLOCK_ID, 0, "", CHUNK_SIZE);
    verify(mBlockWorker, timeout(30000)).createBlockWriter(Sessions.CACHE_WORKER_SESSION_ID,
        BLOCK_ID);
    verify(mBlockWorker, timeout(30000)).commitBlock(Sessions.CACHE_WORKER_SESSION_ID,
        BLOCK_ID, false);
  }

  private void setupMockReaderWriter(String source, int port, long blockId, long blockLength,
      Protocol.OpenUfsBlockOptions options) throws IOException, BlockDoesNotExistException,
      BlockAlreadyExistsException, InvalidWorkerStateException {
    InetSocketAddress sourceAddress = new InetSocketAddress(source, port);
    RemoteBlockReader reader = mock(RemoteBlockReader.class);
    when(mCacheRequestManager.getRemoteBlockReader(blockId, blockLength, sourceAddress, options))
        .thenReturn(reader);
    ReadableByteChannel readerChannel = mock(ReadableByteChannel.class);
    when(reader.getChannel()).thenReturn(readerChannel);
    int bufferSize = 4 * Constants.MB;
    ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
    when(readerChannel.read(buffer)).thenReturn(-1);
    BlockWriter writer = mock(BlockWriter.class);
    when(mBlockWorker.createBlockWriter(Sessions.CACHE_WORKER_SESSION_ID, blockId))
        .thenReturn(writer);
    WritableByteChannel writerChannel = mock(WritableByteChannel.class);
    when(writer.getChannel()).thenReturn(writerChannel);
  }
}
