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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.grpc.GrpcExecutors;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;

/**
 * Unit tests for {@link CacheRequestManager}.
 */
public class CacheRequestManagerTest {
  // block id has to be 0 otherwise would causing start position bug
  private static final long BLOCK_ID = 0L;
  private static final int CHUNK_SIZE = 128;
  private static final int PORT = 22;

  private CacheRequestManager mCacheRequestManager;
  private BlockWorker mBlockWorker;
  private String mRootUfs;
  private final String mLocalWorkerHostname = NetworkAddressUtils.getLocalHostName(
      (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  private Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private final InstancedConfiguration mConf = ServerConfiguration.global();
  private final String mMemDir =
      AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_MEM).getAbsolutePath();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(
      new ImmutableMap.Builder<PropertyKey, String>()
          .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, "1")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "1GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, mMemDir)
          .put(PropertyKey.WORKER_RPC_PORT, "0")
          .put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, AlluxioTestDirectory
              .createTemporaryDirectory("CacheRequestManagerTest").getAbsolutePath())
          .build(),
      mConf);

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws IOException {
    BlockMasterClient blockMasterClient = mock(BlockMasterClient.class);
    BlockMasterClientPool blockMasterClientPool = spy(new BlockMasterClientPool());
    when(blockMasterClientPool.createNewResource()).thenReturn(blockMasterClient);
    TieredBlockStore blockStore = new TieredBlockStore();
    FileSystemMasterClient fileSystemMasterClient = mock(FileSystemMasterClient.class);
    Sessions sessions = mock(Sessions.class);
    // Connect to the real UFS for testing
    UfsManager ufsManager = mock(UfsManager.class);
    mRootUfs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    UfsManager.UfsClient ufsClient = new UfsManager.UfsClient(
        () -> UnderFileSystem.Factory.create(mRootUfs,
            UnderFileSystemConfiguration.defaults(ServerConfiguration.global())),
        new AlluxioURI(mRootUfs));
    when(ufsManager.get(anyLong())).thenReturn(ufsClient);
    mBlockWorker = spy(new DefaultBlockWorker(blockMasterClientPool, fileSystemMasterClient,
        sessions, blockStore, ufsManager));
    FileSystemContext context = mock(FileSystemContext.class);
    mCacheRequestManager =
        spy(new CacheRequestManager(GrpcExecutors.CACHE_MANAGER_EXECUTOR, mBlockWorker, context));
    // Write an actual file to UFS
    String testFilePath = File.createTempFile("temp", null, new File(mRootUfs)).getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray(CHUNK_SIZE);
    BufferUtils.writeBufferToFile(testFilePath, buffer);
    // create options
    BlockInfo info = new BlockInfo().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE);
    URIStatus dummyStatus = new URIStatus(new FileInfo().setPersisted(true).setUfsPath(testFilePath)
        .setBlockIds(Collections.singletonList(BLOCK_ID)).setLength(CHUNK_SIZE)
        .setBlockSizeBytes(CHUNK_SIZE)
        .setFileBlockInfos(Collections.singletonList(new FileBlockInfo().setBlockInfo(info))));
    OpenFilePOptions readOptions = FileSystemOptions.openFileDefaults(mConf);
    InStreamOptions options = new InStreamOptions(dummyStatus, readOptions, mConf);
    mOpenUfsBlockOptions = options.getOpenUfsBlockOptions(BLOCK_ID);
  }

  @Test
  public void submitRequestCacheBlockFromUfs() throws Exception {
    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOpenUfsBlockOptions).setSourceHost(mLocalWorkerHostname)
        .setSourcePort(PORT).build();
    mCacheRequestManager.submitRequest(request);
    assertTrue(mBlockWorker.hasBlockMeta(BLOCK_ID));
  }

  @Test
  public void submitRequestCacheBlockFromRemoteWorker() throws Exception {
    String fakeRemoteWorker = mLocalWorkerHostname + "1";
    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOpenUfsBlockOptions).setSourceHost(fakeRemoteWorker)
        .setSourcePort(PORT).build();
    setupMockRemoteReader(fakeRemoteWorker, PORT, BLOCK_ID, CHUNK_SIZE, mOpenUfsBlockOptions);
    mCacheRequestManager.submitRequest(request);
    assertTrue(mBlockWorker.hasBlockMeta(BLOCK_ID));
  }

  @Test
  public void submitAsyncRequestCacheBlockFromUfs() throws Exception {
    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOpenUfsBlockOptions).setSourceHost(mLocalWorkerHostname)
        .setSourcePort(PORT).setAsync(true).build();
    mCacheRequestManager.submitRequest(request);
    CommonUtils.waitFor("wait for async cache", () -> mBlockWorker.hasBlockMeta(BLOCK_ID));
  }

  @Test
  public void submitAsyncRequestCacheBlockFromRemoteWorker() throws Exception {
    String fakeRemoteWorker = mLocalWorkerHostname + "1";
    CacheRequest request = CacheRequest.newBuilder().setBlockId(BLOCK_ID).setLength(CHUNK_SIZE)
        .setOpenUfsBlockOptions(mOpenUfsBlockOptions).setSourceHost(fakeRemoteWorker)
        .setSourcePort(PORT).setAsync(true).build();
    setupMockRemoteReader(fakeRemoteWorker, PORT, BLOCK_ID, CHUNK_SIZE, mOpenUfsBlockOptions);
    mCacheRequestManager.submitRequest(request);
    CommonUtils.waitFor("wait for async cache", () -> mBlockWorker.hasBlockMeta(BLOCK_ID));
  }

  private void setupMockRemoteReader(String source, int port, long blockId, long blockLength,
      Protocol.OpenUfsBlockOptions options) throws IOException {
    InetSocketAddress sourceAddress = new InetSocketAddress(source, port);
    RemoteBlockReader reader = mock(RemoteBlockReader.class);
    when(mCacheRequestManager.getRemoteBlockReader(blockId, blockLength, sourceAddress, options))
        .thenReturn(reader);
    ReadableByteChannel readerChannel = mock(ReadableByteChannel.class);
    when(reader.getChannel()).thenReturn(readerChannel);
    int bufferSize = 4 * Constants.MB;
    ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
    when(readerChannel.read(buffer)).thenReturn(-1);
  }
}
