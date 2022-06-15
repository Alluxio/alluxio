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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.master.NoopUfsManager;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.IdUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link DefaultBlockWorker}.
 */
public class DefaultBlockWorkerTest {
  private static final int BLOCK_SIZE = 128;

  TieredBlockStore mTieredBlockStore;
  // worker configurations
  private static final long WORKER_ID = 30L;
  // ufs for fallback read
  private static final long UFS_MOUNT_ID = 1L;
  // ufs for batch load
  private static final long UFS_LOAD_MOUNT_ID = 2L;
  private static final WorkerNetAddress WORKER_ADDRESS =
      new WorkerNetAddress().setHost("localhost").setRpcPort(20001);

  // test subject worker
  private DefaultBlockWorker mBlockWorker;

  // mocked dependencies of DefaultBlockWorker
  private BlockMasterClient mBlockMasterClient;
  private FileSystemMasterClient mFileSystemMasterClient;

  private final Random mRandom = new Random();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  // worker's local storage directories
  private String mMemDir;
  private String mHddDir;
  // ufs file for fallback read
  private File mTestUfsFile;

  // ufs root path for batch load
  private String mRootUfs;
  // ufs file for batch load
  private String mTestLoadFilePath;

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
  private BlockStore mBlockStore;

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
    BlockMasterClientPool blockMasterClientPool = spy(new BlockMasterClientPool());
    doReturn(mBlockMasterClient).when(blockMasterClientPool).createNewResource();

    mTieredBlockStore = spy(new TieredBlockStore());
    UfsManager ufsManager = new NoopUfsManager();
    AtomicReference<Long> workerId = new AtomicReference<>(-1L);
    mBlockStore =
        spy(new MonoBlockStore(mTieredBlockStore, blockMasterClientPool, ufsManager, workerId));

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
    ufsManager.addMount(
        UFS_MOUNT_ID,
        new AlluxioURI(mTestUfsFile.getAbsolutePath()),
        UnderFileSystemConfiguration.defaults(Configuration.global())
    );

    mBlockWorker = new DefaultBlockWorker(blockMasterClientPool, mFileSystemMasterClient,
        sessions, mBlockStore, workerId);
    mBlockWorker.start(WORKER_ADDRESS);
  }

  @Test
  public void getWorkerInfo() {
    // block worker has no dependencies
    assertEquals(new HashSet<>(), mBlockWorker.getDependencies());
    // block worker does not expose services
    assertEquals(ImmutableMap.of(), mBlockWorker.getServices());
    assertEquals(WORKER_ID, (long) mBlockWorker.getWorkerId().get());
    assertEquals(Constants.BLOCK_WORKER_NAME, mBlockWorker.getName());
  }

  @Test
  public void abortBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.abortBlock(sessionId, blockId);
    assertFalse(mBlockWorker.getBlockStore().getTempBlockMeta(blockId).isPresent());
  }

  @Test
  public void commitBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    assertFalse(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertTrue(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
  }

  @Test
  public void commitBlockOnRetry() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertTrue(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
  }

  @Test
  public void commitBlockInUfs() throws Exception {
    long blockId = mRandom.nextLong();
    mBlockWorker.commitBlockInUfs(blockId, 1024);

    verify(mBlockMasterClient, times(1)).commitBlockInUfs(blockId, 1024);
  }

  @Test
  public void createBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long initialBytes = 1;
    String path = mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, null, initialBytes));
    assertTrue(path.startsWith(mMemDir)); // tier 0 is mem
  }

  @Test
  public void createBlockOutOfSpace() throws Exception {
    // simulates worker out of space
    doThrow(new WorkerOutOfSpaceException("Out of space"))
        .when(mBlockStore)
        .createBlock(anyLong(), anyLong(), anyInt(), any(CreateBlockOptions.class));

    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();

    assertThrows(
        WorkerOutOfSpaceException.class,
        () -> mBlockWorker.createBlock(
            sessionId,
            blockId,
            0,
            new CreateBlockOptions(null, null, 1)));
  }

  @Test
  public void createBlockLowerTier() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long initialBytes = 1;
    String path = mBlockWorker.createBlock(sessionId, blockId, 1,
        new CreateBlockOptions(null, null, initialBytes));
    assertTrue(path.startsWith(mHddDir));
  }

  @Test
  public void getTempBlockWriter() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    try (BlockWriter blockWriter = mBlockWorker.createBlockWriter(sessionId, blockId)) {
      blockWriter.append(BufferUtils.getIncreasingByteBuffer(10));
      TempBlockMeta meta = mBlockWorker.getBlockStore().getTempBlockMeta(blockId).get();
      assertEquals(Constants.MEDIUM_MEM, meta.getBlockLocation().mediumType());
    }
    mBlockWorker.abortBlock(sessionId, blockId);
  }

  @Test
  public void getReport() {
    BlockHeartbeatReport report = mBlockWorker.getReport();
    assertEquals(0, report.getAddedBlocks().size());
    assertEquals(0, report.getRemovedBlocks().size());
  }

  @Test
  public void getStoreMeta() throws Exception {
    long blockId1 = mRandom.nextLong();
    long blockId2 = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId1, 0, new CreateBlockOptions(null, "", 1L));
    mBlockWorker.createBlock(sessionId, blockId2, 1, new CreateBlockOptions(null, "", 1L));

    BlockStoreMeta storeMeta = mBlockWorker.getStoreMetaFull();
    assertEquals(2, storeMeta.getBlockList().size());
    assertEquals(2, storeMeta.getBlockListByStorageLocation().size());
    assertEquals(0, storeMeta.getBlockList().get("MEM").size());
    assertEquals(3L * Constants.GB, storeMeta.getCapacityBytes());
    assertEquals(2L, storeMeta.getUsedBytes());
    assertEquals(1L, storeMeta.getUsedBytesOnTiers().get("MEM").longValue());
    assertEquals(1L, storeMeta.getUsedBytesOnTiers().get("HDD").longValue());

    BlockStoreMeta storeMeta2 = mBlockWorker.getStoreMeta();
    assertEquals(3L * Constants.GB, storeMeta2.getCapacityBytes());
    assertEquals(2L, storeMeta2.getUsedBytes());

    mBlockWorker.commitBlock(sessionId, blockId1, true);
    mBlockWorker.commitBlock(sessionId, blockId2, true);

    storeMeta = mBlockWorker.getStoreMetaFull();
    assertEquals(1, storeMeta.getBlockList().get("MEM").size());
    assertEquals(1, storeMeta.getBlockList().get("HDD").size());
    Map<BlockStoreLocation, List<Long>> blockLocations = storeMeta.getBlockListByStorageLocation();
    assertEquals(1, blockLocations.get(
            new BlockStoreLocation("MEM", 0, "MEM")).size());
    assertEquals(1, blockLocations.get(
            new BlockStoreLocation("HDD", 0, "HDD")).size());
    assertEquals(2, storeMeta.getNumberOfBlocks());
  }

  @Test
  public void hasBlockMeta() throws Exception  {
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    assertFalse(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertTrue(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
  }

  @Test
  public void removeBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 1, new CreateBlockOptions(null, "", 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    mBlockWorker.removeBlock(sessionId, blockId);
    assertFalse(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
  }

  @Test
  public void requestSpace() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long initialBytes = 512;
    long additionalBytes = 1024;
    mBlockWorker.createBlock(sessionId, blockId, 1, new CreateBlockOptions(null, "", initialBytes));
    mBlockWorker.requestSpace(sessionId, blockId, additionalBytes);
    assertEquals(initialBytes + additionalBytes,
        mBlockWorker.getBlockStore().getTempBlockMeta(blockId).get().getBlockSize());
  }

  @Test
  public void requestSpaceNoBlock() {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long additionalBytes = 1;
    assertThrows(IllegalStateException.class,
        () -> mBlockWorker.requestSpace(sessionId, blockId, additionalBytes)
    );
  }

  @Test
  public void requestSpaceNoSpace() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long additionalBytes = 2L * Constants.GB + 1;
    mBlockWorker.createBlock(sessionId, blockId, 1, new CreateBlockOptions(null, "", 1));
    assertThrows(WorkerOutOfSpaceException.class,
        () -> mBlockWorker.requestSpace(sessionId, blockId, additionalBytes)
    );
  }

  @Test
  public void updatePinList() {
    Set<Long> pinnedInodes = new HashSet<>();
    pinnedInodes.add(mRandom.nextLong());

    mBlockWorker.updatePinList(pinnedInodes);
    verify(mTieredBlockStore).updatePinnedInodes(pinnedInodes);
  }

  @Test
  public void getFileInfo() throws Exception {
    long fileId = mRandom.nextLong();
    mBlockWorker.getFileInfo(fileId);
    verify(mFileSystemMasterClient).getFileInfo(fileId);
  }

  @Test
  public void getBlockReader() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    BlockReader reader = mBlockWorker.createBlockReader(IdUtils.createSessionId(), blockId, 0,
        false, Protocol.OpenUfsBlockOptions.newBuilder().build());
    // reader will hold the lock
    assertThrows(DeadlineExceededException.class,
        () -> mTieredBlockStore.removeBlockInternal(sessionId, blockId, 10)
    );
    reader.close();
    mTieredBlockStore.removeBlockInternal(sessionId, blockId, 10);
  }

  @Test
  public void loadMultipleFromUfs() throws IOException {
    Block block =
        Block.newBuilder().setBlockId(0).setBlockSize(BLOCK_SIZE)
            .setMountId(UFS_LOAD_MOUNT_ID).setOffsetInFile(0).setUfsPath(mTestLoadFilePath).build();
    Block block2 = Block.newBuilder().setBlockId(1).setBlockSize(BLOCK_SIZE / 2)
        .setMountId(UFS_LOAD_MOUNT_ID).setOffsetInFile(BLOCK_SIZE)
        .setUfsPath(mTestLoadFilePath).build();

    List<BlockStatus> res =
        mBlockWorker.load(Arrays.asList(block, block2), "test", OptionalInt.empty());
    assertEquals(res.size(), 0);
    assertTrue(mBlockStore.hasBlockMeta(0));
    assertTrue(mBlockStore.hasBlockMeta(1));
    BlockReader reader = mBlockWorker.createBlockReader(0, 0, 0, false,
        Protocol.OpenUfsBlockOptions.getDefaultInstance());
    // Read entire block by setting the length to be block size.
    ByteBuffer buffer = reader.read(0, BLOCK_SIZE);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(0, BLOCK_SIZE, buffer));
    reader = mBlockWorker.createBlockReader(0, 1, 0, false,
        Protocol.OpenUfsBlockOptions.getDefaultInstance());
    buffer = reader.read(0, BLOCK_SIZE / 2);
    assertTrue(BufferUtils.equalIncreasingByteBuffer(BLOCK_SIZE, BLOCK_SIZE / 2, buffer));
  }

  @Test
  public void loadDuplicateBlock() {
    int blockId = 0;
    Block blocks = Block.newBuilder().setBlockId(blockId).setBlockSize(BLOCK_SIZE)
        .setMountId(UFS_LOAD_MOUNT_ID).setOffsetInFile(0).setUfsPath(mTestLoadFilePath).build();
    List<BlockStatus> res =
        mBlockWorker.load(Collections.singletonList(blocks), "test", OptionalInt.empty());
    assertEquals(res.size(), 0);
    List<BlockStatus> failure =
        mBlockWorker.load(Collections.singletonList(blocks), "test", OptionalInt.empty());
    assertEquals(failure.size(), 1);
  }

  @Test
  public void getFallBackUfsReader() throws Exception {
    long ufsBlockSize = 1024;
    // flush some data to under file system
    byte[] data = new byte[(int) ufsBlockSize];
    Arrays.fill(data, (byte) 7);
    try (FileOutputStream fileOut = new FileOutputStream(mTestUfsFile);
         BufferedOutputStream bufOut = new BufferedOutputStream(fileOut)) {
      bufOut.write(data);
      bufOut.flush();
    }

    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    Protocol.OpenUfsBlockOptions options = Protocol.OpenUfsBlockOptions
        .newBuilder()
        .setMountId(UFS_MOUNT_ID)
        .setBlockSize(ufsBlockSize)
        .setUfsPath(mTestUfsFile.getAbsolutePath())
        .setBlockInUfsTier(true)
        .build();

    // this read should fall back to ufs
    BlockReader reader = mBlockWorker.createBlockReader(
        sessionId, blockId, 0, false, options);

    // read a whole block
    assertArrayEquals(data, reader.read(0, ufsBlockSize).array());
    reader.close();

    // after closing, the ufs block should be cached locally
    assertTrue(mBlockWorker.getBlockStore().hasBlockMeta(blockId));
  }

  @Test
  public void getUnavailableBlockReader() {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();

    // access a non-existent non-ufs block
    assertThrows(
        BlockDoesNotExistRuntimeException.class,
        () -> mBlockWorker.createBlockReader(
            sessionId, blockId, 0, false, null));
  }

  @Test
  public void getUnavailableUfsBlockReader() {
    // try access a non-existent ufs path
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    Protocol.OpenUfsBlockOptions options = Protocol.OpenUfsBlockOptions
        .newBuilder()
        .setUfsPath("/nonexistent/path")
        .setBlockInUfsTier(true)
        .setBlockSize(1024)
        .setMountId(UFS_MOUNT_ID)
        .build();

    assertThrows(UnavailableException.class,
        () -> mBlockWorker.createUfsBlockReader(
            sessionId, blockId, 0, false, options));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void blockMasterSync() throws Exception {
    // verify that syncing heartbeat is working properly
    Thread.sleep(10 * Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    // BlockWorker should have fired 10 calls of heartbeat during this interval
    // check that at least 5 calls are made to make room for some scheduling issues.
    verify(mBlockMasterClient, atLeast(5)).heartbeat(
        eq(WORKER_ID),
        any(Map.class),
        any(Map.class),
        any(List.class),
        any(Map.class),
        any(Map.class),
        any(List.class)
    );
  }

  @Test
  public void pinListSync() throws Exception {
    // verify that pin list syncing is working properly
    Thread.sleep(10 * Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    verify(mFileSystemMasterClient, atLeast(5)).getPinList();
  }

  @After
  public void after() throws Exception {
    mBlockWorker.stop();
  }

  // create a BlockMasterClient that simulates reasonable default
  // interactions with the block master
  @SuppressWarnings("unchecked")
  private BlockMasterClient createMockBlockMasterClient() throws Exception {
    BlockMasterClient client = mock(BlockMasterClient.class);

    // return designated worker id
    doReturn(WORKER_ID)
        .when(client)
        .getId(any(WorkerNetAddress.class));

    // return Command.Nothing for heartbeat
    doReturn(Command.newBuilder().setCommandType(CommandType.Nothing).build())
        .when(client)
        .heartbeat(
            any(long.class),
            any(Map.class),
            any(Map.class),
            any(List.class),
            any(Map.class),
            any(Map.class),
            any(List.class)
        );
    return client;
  }

  // create a mocked FileSystemMasterClient that simulates reasonable default
  // interactions with file system master
  private FileSystemMasterClient createMockFileSystemMasterClient() throws Exception {
    FileSystemMasterClient client = mock(FileSystemMasterClient.class);
    doReturn(ImmutableSet.of())
        .when(client)
        .getPinList();
    return client;
  }
}
