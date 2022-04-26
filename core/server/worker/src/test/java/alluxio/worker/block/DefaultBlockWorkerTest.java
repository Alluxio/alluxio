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

import static alluxio.worker.block.BlockWorker.INVALID_LOCK_ID;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.grpc.ReadRequest;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Unit tests for {@link DefaultBlockWorker}.
 */
public class DefaultBlockWorkerTest {
  private BlockMasterClient mBlockMasterClient;
  private BlockMasterClientPool mBlockMasterClientPool;
  private TieredBlockStore mBlockStore;
  private DefaultBlockWorker mBlockWorker;
  private FileSystemMasterClient mFileSystemMasterClient;
  private Random mRandom;
  private Sessions mSessions;
  private UfsManager mUfsManager;
  private String mMemDir =
      AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_MEM).getAbsolutePath();
  private String mHddDir =
      AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_HDD).getAbsolutePath();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, Object>()
          .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, 2)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "1GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, mMemDir)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS, Constants.MEDIUM_HDD)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_MEDIUMTYPE, Constants.MEDIUM_HDD)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA, "2GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, mHddDir)
          .put(PropertyKey.WORKER_RPC_PORT, 0)
          .put(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES, "0")
          .build(), ServerConfiguration.global());

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws IOException {
    mRandom = new Random();
    mBlockMasterClient = mock(BlockMasterClient.class);
    mBlockMasterClientPool = spy(new BlockMasterClientPool());
    when(mBlockMasterClientPool.createNewResource()).thenReturn(mBlockMasterClient);
    mBlockStore = spy(new TieredBlockStore());
    mFileSystemMasterClient = mock(FileSystemMasterClient.class);
    mSessions = mock(Sessions.class);
    mUfsManager = mock(UfsManager.class);

    mBlockWorker = new DefaultBlockWorker(mBlockMasterClientPool, mFileSystemMasterClient,
        mSessions, mBlockStore, mUfsManager);
  }

  @Test
  public void openUnderFileSystemBlock() throws Exception {
    long blockId = mRandom.nextLong();
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();

    long sessionId = 1;
    for (; sessionId < 11; sessionId++) {
      assertTrue(mBlockWorker.openUfsBlock(sessionId, blockId, openUfsBlockOptions));
    }
    assertFalse(mBlockWorker.openUfsBlock(sessionId, blockId, openUfsBlockOptions));
  }

  @Test
  public void closeUnderFileSystemBlock() throws Exception {
    long blockId = mRandom.nextLong();
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setMaxUfsReadConcurrency(10).setUfsPath("/a").build();

    long sessionId = 1;
    for (; sessionId < 11; sessionId++) {
      assertTrue(mBlockWorker.openUfsBlock(sessionId, blockId, openUfsBlockOptions));
      mBlockWorker.closeUfsBlock(sessionId, blockId);
    }
    assertTrue(mBlockWorker.openUfsBlock(sessionId, blockId, openUfsBlockOptions));
  }

  @Test
  public void abortBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.abortBlock(sessionId, blockId);
    assertThrows(BlockDoesNotExistException.class, () -> mBlockWorker.getTempBlockMeta(blockId));
  }

  @Test
  public void accessBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    mBlockWorker.accessBlock(sessionId, blockId);
    verify(mBlockStore).accessBlock(sessionId, blockId);
  }

  @Test
  public void commitBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    assertFalse(mBlockWorker.hasBlockMeta(blockId));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertTrue(mBlockWorker.hasBlockMeta(blockId));
  }

  @Test
  public void commitBlockOnRetry() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertTrue(mBlockWorker.hasBlockMeta(blockId));
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
      TempBlockMeta meta = mBlockWorker.getTempBlockMeta(blockId);
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
  public void getVolatileBlockMetaNotFound() throws Exception {
    long blockId = mRandom.nextLong();
    assertThrows(BlockDoesNotExistException.class,
        () -> mBlockWorker.getVolatileBlockMeta(blockId));
  }

  @Test
  public void getVolatileBlockMeta() throws Exception {
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertEquals(blockId, mBlockWorker.getVolatileBlockMeta(blockId).getBlockId());
  }

  @Test
  public void getBlockMetaNotFound() throws Exception {
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    assertThrows(BlockDoesNotExistException.class,
        () -> mBlockWorker.getBlockMeta(sessionId, blockId, lockId)
    );
  }

  @Test
  public void getBlockMeta() throws Exception {
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    long lockId = mBlockWorker.lockBlock(sessionId, blockId);
    assertEquals(blockId, mBlockWorker.getBlockMeta(sessionId, blockId, lockId).getBlockId());
  }

  @Test
  public void hasBlockMeta() throws Exception  {
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    assertFalse(mBlockWorker.hasBlockMeta(blockId));
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertTrue(mBlockWorker.hasBlockMeta(blockId));
  }

  @Test
  public void lockBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    assertThrows(BlockDoesNotExistException.class,
        () -> mBlockWorker.lockBlock(sessionId, blockId));
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    assertNotEquals(INVALID_LOCK_ID, mBlockWorker.lockBlock(sessionId, blockId));
  }

  @Test
  public void moveBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 1, new CreateBlockOptions(null, "", 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    // move sure this block is on tier 1
    mBlockWorker.moveBlock(sessionId, blockId, 1);
    assertEquals(Constants.MEDIUM_HDD,
        mBlockWorker.getVolatileBlockMeta(blockId).getBlockLocation().tierAlias());
    mBlockWorker.moveBlock(sessionId, blockId, 0);
    assertEquals(Constants.MEDIUM_MEM,
        mBlockWorker.getVolatileBlockMeta(blockId).getBlockLocation().tierAlias());
  }

  @Test
  public void removeBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 1, new CreateBlockOptions(null, "", 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    mBlockWorker.removeBlock(sessionId, blockId);
    assertFalse(mBlockWorker.hasBlockMeta(blockId));
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
        mBlockWorker.getTempBlockMeta(blockId).getBlockSize());
  }

  @Test
  public void requestSpaceNoBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long additionalBytes = 1;
    assertThrows(BlockDoesNotExistException.class,
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
  public void unlockBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.createBlock(sessionId, blockId, 0,
        new CreateBlockOptions(null, Constants.MEDIUM_MEM, 1));
    mBlockWorker.commitBlock(sessionId, blockId, true);
    long lockId = mBlockWorker.lockBlock(sessionId, blockId);
    assertNotNull(mBlockWorker.getBlockMeta(sessionId, blockId, lockId));
    mBlockWorker.unlockBlock(lockId);
    assertThrows(BlockDoesNotExistException.class,
        () -> mBlockWorker.getBlockMeta(sessionId, blockId, lockId));
  }

  @Test
  public void updatePinList() {
    Set<Long> pinnedInodes = new HashSet<>();
    pinnedInodes.add(mRandom.nextLong());

    mBlockWorker.updatePinList(pinnedInodes);
    verify(mBlockStore).updatePinnedInodes(pinnedInodes);
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
    BlockReadRequest request = new BlockReadRequest(
        ReadRequest.newBuilder().setBlockId(blockId).setOffset(0).setLength(10).build());
    BlockReader reader = mBlockWorker.createBlockReader(request);
    // reader will hold the lock
    assertThrows(DeadlineExceededException.class,
        () -> mBlockStore.removeBlockInternal(sessionId, blockId, BlockStoreLocation.anyTier(), 10)
    );
    reader.close();
    mBlockStore.removeBlockInternal(sessionId, blockId, BlockStoreLocation.anyTier(), 10);
  }
}
