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

import static alluxio.util.CommonUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.master.NoopUfsManager;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link DefaultBlockWorker}.
 */
public class DefaultBlockWorkerTestBase {
  protected static final int BLOCK_SIZE = 128;

  TieredBlockStore mTieredBlockStore;
  // worker configurations
  protected static final long WORKER_ID = 30L;
  // ufs for fallback read
  protected static final long UFS_MOUNT_ID = 1L;
  // ufs for batch load
  protected static final long UFS_LOAD_MOUNT_ID = 2L;
  protected static final WorkerNetAddress WORKER_ADDRESS =
      new WorkerNetAddress().setHost("localhost").setRpcPort(20001);

  // invalid initial worker id
  protected static final long INVALID_WORKER_ID = -1L;

  // test subject
  protected DefaultBlockWorker mBlockWorker;

  // mocked dependencies of DefaultBlockWorker
  protected BlockMasterClient mBlockMasterClient;
  protected FileSystemMasterClient mFileSystemMasterClient;

  protected final Random mRandom = new Random();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  // worker's local storage directories
  protected String mMemDir;
  protected String mHddDir;
  // ufs file for fallback read
  protected File mTestUfsFile;

  // ufs root path for batch load
  protected String mRootUfs;
  // ufs file for batch load
  protected String mTestLoadFilePath;
  protected BlockMasterClientPool mBlockMasterClientPool;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, Object>()
          .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, 2)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "1GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS, Constants.MEDIUM_HDD)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_MEDIUMTYPE, Constants.MEDIUM_HDD)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA, "2GB")
          .put(PropertyKey.WORKER_RPC_PORT, 0)
          .put(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES, "0")
          .put(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, "10ms")
          .build(), Configuration.modifiableGlobal());
  protected BlockStore mBlockStore;

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // set up storage directories
    mMemDir = mTestFolder.newFolder().getAbsolutePath();
    mHddDir = mTestFolder.newFolder().getAbsolutePath();
    mConfigurationRule.set(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, mMemDir);
    mConfigurationRule.set(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, mHddDir);

    // set up BlockMasterClient
    mBlockMasterClient = createMockBlockMasterClient();
    mBlockMasterClientPool = spy(new BlockMasterClientPool());
    doReturn(mBlockMasterClient).when(mBlockMasterClientPool).createNewResource();

    mTieredBlockStore = spy(new TieredBlockStore());
    UfsManager ufsManager = new NoopUfsManager();
    AtomicReference<Long> workerId = new AtomicReference<>(INVALID_WORKER_ID);
    mBlockStore =
        spy(new MonoBlockStore(mTieredBlockStore, mBlockMasterClientPool, ufsManager, workerId));

    mFileSystemMasterClient = createMockFileSystemMasterClient();

    Sessions sessions = mock(Sessions.class);

    // set up a ufs directory for batch load jobs
    mRootUfs = mTestFolder.newFolder("DefaultBlockWorkerTest").getAbsolutePath();
    mConfigurationRule.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mRootUfs);
    ufsManager.addMount(UFS_LOAD_MOUNT_ID,
        new AlluxioURI(mRootUfs),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    // Write an actual file to UFS
    mTestLoadFilePath = mTestFolder.newFile("temp").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) (BLOCK_SIZE * 1.5));
    BufferUtils.writeBufferToFile(mTestLoadFilePath, buffer);

    // set up ufs directory for fallback reading
    mTestUfsFile = mTestFolder.newFile();
    // mount test file to UFS_MOUNT_ID
    ufsManager.addMount(
        UFS_MOUNT_ID,
        new AlluxioURI(mTestUfsFile.getAbsolutePath()),
        UnderFileSystemConfiguration.defaults(Configuration.global())
    );

    mBlockWorker = new DefaultBlockWorker(mBlockMasterClientPool, mFileSystemMasterClient,
        sessions, mBlockStore, workerId);
  }

  protected void cacheBlock(boolean async) throws Exception {
    // flush 1MB random data to ufs so that caching will take a while
    long ufsBlockSize = 1024 * 1024;
    byte[] data = new byte[(int) ufsBlockSize];
    mRandom.nextBytes(data);

    try (FileOutputStream fileOut = new FileOutputStream(mTestUfsFile);
         BufferedOutputStream bufOut = new BufferedOutputStream(fileOut)) {
      bufOut.write(data);
      bufOut.flush();
    }

    // ufs options: delegate to the ufs mounted at UFS_MOUNT_ID
    // with path to our test file
    long blockId = mRandom.nextLong();
    Protocol.OpenUfsBlockOptions options = Protocol.OpenUfsBlockOptions
        .newBuilder()
        .setBlockSize(ufsBlockSize)
        .setUfsPath(mTestUfsFile.getAbsolutePath())
        .setMountId(UFS_MOUNT_ID)
        .setNoCache(false)
        .setOffsetInFile(0)
        .build();

    // cache request:
    // delegate to local ufs client rather than remote worker
    CacheRequest request = CacheRequest
        .newBuilder()
        .setSourceHost(NetworkAddressUtils.getLocalHostName(500))
        .setBlockId(blockId)
        .setLength(ufsBlockSize)
        .setAsync(async)
        .setOpenUfsBlockOptions(options)
        .build();

    mBlockWorker.cache(request);

    // check that the block metadata is present
    if (async) {
      assertFalse(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
      waitFor(
          "Wait for async cache",
          () -> mBlockWorker.getBlockStore().hasBlockMeta(blockId),
          WaitForOptions.defaults().setInterval(10).setTimeoutMs(2000));
    } else {
      assertTrue(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
    }

    long sessionId = mRandom.nextLong();
    // check that we can read the block locally
    // note: this time we use an OpenUfsOption without ufsPath and blockInUfsTier so
    // that the worker can't fall back to ufs read.
    Protocol.OpenUfsBlockOptions noFallbackOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setBlockInUfsTier(false).build();
    try (BlockReader reader = mBlockWorker.createBlockReader(
        sessionId, blockId, 0, false, noFallbackOptions)) {
      ByteBuffer buf = reader.read(0, ufsBlockSize);
      // alert: LocalFileBlockReader uses a MappedByteBuffer, which does not
      // support the array operation. So we need to compare ByteBuffer manually
      assertEquals(0, buf.compareTo(ByteBuffer.wrap(data)));
    }
  }

  // create a BlockMasterClient that simulates reasonable default
  // interactions with the block master
  protected BlockMasterClient createMockBlockMasterClient() throws Exception {
    BlockMasterClient client = mock(BlockMasterClient.class);

    // return designated worker id
    doReturn(WORKER_ID)
        .when(client)
        .getId(any(WorkerNetAddress.class));

    // return Command.Nothing for heartbeat
    doReturn(Command.newBuilder().setCommandType(CommandType.Nothing).build())
        .when(client)
        .heartbeat(
            anyLong(),
            anyMap(),
            anyMap(),
            anyList(),
            anyMap(),
            anyMap(),
            anyList()
        );
    return client;
  }

  // create a mocked FileSystemMasterClient that simulates reasonable default
  // interactions with file system master
  protected FileSystemMasterClient createMockFileSystemMasterClient() throws Exception {
    FileSystemMasterClient client = mock(FileSystemMasterClient.class);
    doReturn(ImmutableSet.of())
        .when(client)
        .getPinList();
    return client;
  }
}
