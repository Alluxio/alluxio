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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Unit tests for {@link DefaultBlockWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, BlockMasterClientPool.class, FileSystemMasterClient.class,
    BlockHeartbeatReporter.class, BlockMetricsReporter.class, BlockMeta.class,
    BlockStoreLocation.class, StorageDir.class, Configuration.class, UnderFileSystem.class,
    BlockWorker.class, Sessions.class})
public class BlockWorkerTest {

  private BlockMasterClient mBlockMasterClient;
  private BlockMasterClientPool mBlockMasterClientPool;
  private BlockStore mBlockStore;
  private FileSystemMasterClient mFileSystemMasterClient;
  private Random mRandom;
  private Sessions mSessions;
  private BlockWorker mBlockWorker;
  private UfsManager mUfsManager;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, String>()
          .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2")
          .put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1),
              String.valueOf(Constants.GB))
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH,
              AlluxioTestDirectory.createTemporaryDirectory("WORKER_TIERED_STORE_LEVEL0_DIRS_PATH")
                  .getAbsolutePath())
          .put(PropertyKey.WORKER_DATA_PORT, Integer.toString(0))
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS, "HDD")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, AlluxioTestDirectory
              .createTemporaryDirectory("WORKER_TIERED_STORE_LEVEL1_DIRS_PATH").getAbsolutePath())
          .build());

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws IOException {
    mRandom = new Random();
    mBlockMasterClient = PowerMockito.mock(BlockMasterClient.class);
    mBlockMasterClientPool =
        Mockito.spy(new BlockMasterClientPool(new InetSocketAddress("localhost", 12345)));
    Mockito.when(mBlockMasterClientPool.createNewResource()).thenReturn(mBlockMasterClient);
    mBlockStore = PowerMockito.mock(BlockStore.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    mSessions = PowerMockito.mock(Sessions.class);
    mUfsManager = Mockito.mock(UfsManager.class);

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

  /**
   * Tests the {@link BlockWorker#abortBlock(long, long)} method.
   */
  @Test
  public void abortBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.abortBlock(sessionId, blockId);
    verify(mBlockStore).abortBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#accessBlock(long, long)} method.
   */
  @Test
  public void accessBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.accessBlock(sessionId, blockId);
    verify(mBlockStore).accessBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#commitBlock(long, long)} method.
   */
  @Test
  public void commitBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long length = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long usedBytes = mRandom.nextLong();
    String tierAlias = "MEM";
    HashMap<String, Long> usedBytesOnTiers = new HashMap<>();
    usedBytesOnTiers.put(tierAlias, usedBytes);
    BlockMeta blockMeta = PowerMockito.mock(BlockMeta.class);
    BlockStoreLocation blockStoreLocation = PowerMockito.mock(BlockStoreLocation.class);
    BlockStoreMeta blockStoreMeta = Mockito.mock(BlockStoreMeta.class);

    when(mBlockStore.lockBlock(sessionId, blockId)).thenReturn(lockId);
    when(mBlockStore.getBlockMeta(sessionId, blockId, lockId)).thenReturn(blockMeta);
    when(mBlockStore.getBlockStoreMeta()).thenReturn(blockStoreMeta);
    when(mBlockStore.getBlockStoreMetaFull()).thenReturn(blockStoreMeta);
    when(blockMeta.getBlockLocation()).thenReturn(blockStoreLocation);
    when(blockStoreLocation.tierAlias()).thenReturn(tierAlias);
    when(blockMeta.getBlockSize()).thenReturn(length);
    when(blockStoreMeta.getUsedBytesOnTiers()).thenReturn(usedBytesOnTiers);

    mBlockWorker.commitBlock(sessionId, blockId);
    verify(mBlockMasterClient).commitBlock(anyLong(), eq(usedBytes), eq(tierAlias), eq(blockId),
        eq(length));
    verify(mBlockStore).unlockBlock(lockId);
  }

  /**
   * Tests that commitBlock doesn't throw an exception when {@link BlockAlreadyExistsException} gets
   * thrown by the block store.
   */
  @Test
  public void commitBlockOnRetry() throws Exception {
    long blockId = mRandom.nextLong();
    long length = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long usedBytes = mRandom.nextLong();
    String tierAlias = "MEM";
    HashMap<String, Long> usedBytesOnTiers = new HashMap<>();
    usedBytesOnTiers.put(tierAlias, usedBytes);
    BlockMeta blockMeta = PowerMockito.mock(BlockMeta.class);
    BlockStoreLocation blockStoreLocation = PowerMockito.mock(BlockStoreLocation.class);
    BlockStoreMeta blockStoreMeta = Mockito.mock(BlockStoreMeta.class);

    when(mBlockStore.lockBlock(sessionId, blockId)).thenReturn(lockId);
    when(mBlockStore.getBlockMeta(sessionId, blockId, lockId)).thenReturn(blockMeta);
    when(mBlockStore.getBlockStoreMeta()).thenReturn(blockStoreMeta);
    when(blockMeta.getBlockLocation()).thenReturn(blockStoreLocation);
    when(blockStoreLocation.tierAlias()).thenReturn(tierAlias);
    when(blockMeta.getBlockSize()).thenReturn(length);
    when(blockStoreMeta.getUsedBytesOnTiers()).thenReturn(usedBytesOnTiers);

    doThrow(new BlockAlreadyExistsException("")).when(mBlockStore).commitBlock(sessionId, blockId);
    mBlockWorker.commitBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#createBlock(long, long, String, long)} method.
   */
  @Test
  public void createBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    TempBlockMeta meta = new TempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlock(sessionId, blockId, location, initialBytes)).thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(
        PathUtils.concatPath("/tmp", ".tmp_blocks", sessionId % 1024,
            String.format("%x-%x", sessionId, blockId)),
        mBlockWorker.createBlock(sessionId, blockId, tierAlias, initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#createBlock(long, long, String, long)} method with a tier other
   * than MEM.
   */
  @Test
  public void createBlockLowerTier() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "HDD";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    TempBlockMeta meta = new TempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlock(sessionId, blockId, location, initialBytes)).thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(
        PathUtils.concatPath("/tmp", ".tmp_blocks", sessionId % 1024,
            String.format("%x-%x", sessionId, blockId)),
        mBlockWorker.createBlock(sessionId, blockId, tierAlias, initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#createBlockRemote(long, long, String, long)} method.
   */
  @Test
  public void createBlockRemote() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    TempBlockMeta meta = new TempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlock(sessionId, blockId, location, initialBytes)).thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(
        PathUtils.concatPath("/tmp", ".tmp_blocks", sessionId % 1024,
            String.format("%x-%x", sessionId, blockId)),
        mBlockWorker.createBlock(sessionId, blockId, tierAlias, initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#freeSpace(long, long, String)} method.
   */
  @Test
  public void freeSpace() throws Exception {
    long sessionId = mRandom.nextLong();
    long availableBytes = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockWorker.freeSpace(sessionId, availableBytes, tierAlias);
    verify(mBlockStore).freeSpace(sessionId, availableBytes, location);
  }

  /**
   * Tests the {@link BlockWorker#getTempBlockWriterRemote(long, long)} method.
   */
  @Test
  public void getTempBlockWriterRemote() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.getTempBlockWriterRemote(sessionId, blockId);
    verify(mBlockStore).getBlockWriter(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#getReport()} method.
   */
  @Test
  public void getReport() {
    BlockHeartbeatReport report = mBlockWorker.getReport();
    assertEquals(0, report.getAddedBlocks().size());
    assertEquals(0, report.getRemovedBlocks().size());
  }

  /**
   * Tests the {@link BlockWorker#getStoreMeta()} method.
   *
   */
  @Test
  public void getStoreMeta() {
    mBlockWorker.getStoreMeta();
    mBlockWorker.getStoreMetaFull();
    verify(mBlockStore).getBlockStoreMeta();
    verify(mBlockStore).getBlockStoreMetaFull();
  }

  /**
   * Tests the {@link BlockWorker#getVolatileBlockMeta(long)} method.
   */
  @Test
  public void getVolatileBlockMeta() throws Exception {
    long blockId = mRandom.nextLong();
    mBlockWorker.getVolatileBlockMeta(blockId);
    verify(mBlockStore).getVolatileBlockMeta(blockId);
  }

  /**
   * Tests the {@link BlockWorker#getBlockMeta(long, long, long)} method.
   */
  @Test
  public void getBlockMeta() throws Exception {
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    mBlockWorker.getBlockMeta(sessionId, blockId, lockId);
    verify(mBlockStore).getBlockMeta(sessionId, blockId, lockId);
  }

  /**
   * Tests the {@link BlockWorker#hasBlockMeta(long)} method.
   */
  @Test
  public void hasBlockMeta() {
    long blockId = mRandom.nextLong();
    mBlockWorker.hasBlockMeta(blockId);
    verify(mBlockStore).hasBlockMeta(blockId);
  }

  /**
   * Tests the {@link BlockWorker#lockBlock(long, long)} method.
   */
  @Test
  public void lockBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.lockBlock(sessionId, blockId);
    verify(mBlockStore).lockBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#moveBlock(long, long, String)} method.
   */
  @Test
  public void moveBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    BlockStoreLocation existingLocation = Mockito.mock(BlockStoreLocation.class);
    when(existingLocation.belongsTo(location)).thenReturn(false);
    BlockMeta meta = Mockito.mock(BlockMeta.class);
    when(meta.getBlockLocation()).thenReturn(existingLocation);
    when(mBlockStore.getBlockMeta(Mockito.eq(sessionId), Mockito.eq(blockId), Mockito.anyLong()))
        .thenReturn(meta);
    mBlockWorker.moveBlock(sessionId, blockId, tierAlias);
    verify(mBlockStore).moveBlock(sessionId, blockId, location);
  }

  /**
   * Tests the {@link BlockWorker#moveBlock(long, long, String)} method no-ops if the block is
   * already at the destination location.
   */
  @Test
  public void moveBlockNoop() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    BlockStoreLocation existingLocation = Mockito.mock(BlockStoreLocation.class);
    when(existingLocation.belongsTo(location)).thenReturn(true);
    BlockMeta meta = Mockito.mock(BlockMeta.class);
    when(meta.getBlockLocation()).thenReturn(existingLocation);
    when(mBlockStore.getBlockMeta(Mockito.eq(sessionId), Mockito.eq(blockId), Mockito.anyLong()))
        .thenReturn(meta);
    mBlockWorker.moveBlock(sessionId, blockId, tierAlias);
    verify(mBlockStore, Mockito.times(0)).moveBlock(sessionId, blockId, location);
  }

  /**
   * Tests the {@link BlockWorker#readBlock(long, long, long)} method.
   */
  @Test
  public void readBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    long blockSize = mRandom.nextLong();
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    BlockMeta meta = new BlockMeta(blockId, blockSize, storageDir);
    when(mBlockStore.getBlockMeta(sessionId, blockId, lockId)).thenReturn(meta);

    mBlockWorker.readBlock(sessionId, blockId, lockId);
    verify(mBlockStore).getBlockMeta(sessionId, blockId, lockId);
    assertEquals(PathUtils.concatPath("/tmp", blockId),
        mBlockWorker.readBlock(sessionId, blockId, lockId));
  }

  /**
   * Tests the {@link BlockWorker#readBlockRemote(long, long, long)} method.
   */
  @Test
  public void readBlockRemote() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long lockId = mRandom.nextLong();

    mBlockWorker.readBlockRemote(sessionId, blockId, lockId);
    verify(mBlockStore).getBlockReader(sessionId, blockId, lockId);
  }

  /**
   * Tests the {@link BlockWorker#removeBlock(long, long)} method.
   */
  @Test
  public void removeBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.removeBlock(sessionId, blockId);
    verify(mBlockStore).removeBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#requestSpace(long, long, long)} method.
   */
  @Test
  public void requestSpace() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long additionalBytes = mRandom.nextLong();
    mBlockWorker.requestSpace(sessionId, blockId, additionalBytes);
    verify(mBlockStore).requestSpace(sessionId, blockId, additionalBytes);
  }

  /**
   * Tests the {@link BlockWorker#unlockBlock(long)} and {@link BlockWorker#unlockBlock(long, long)}
   * method.
   */
  @Test
  public void unlockBlock() throws Exception {
    long lockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();

    mBlockWorker.unlockBlock(lockId);
    verify(mBlockStore).unlockBlock(lockId);

    mBlockWorker.unlockBlock(sessionId, blockId);
    verify(mBlockStore).unlockBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#sessionHeartbeat(long)} method.
   */
  @Test
  public void sessionHeartbeat() {
    long sessionId = mRandom.nextLong();

    mBlockWorker.sessionHeartbeat(sessionId);
    verify(mSessions).sessionHeartbeat(sessionId);
  }

  /**
   * Tests the {@link BlockWorker#updatePinList(Set)} method.
   */
  @Test
  public void updatePinList() {
    Set<Long> pinnedInodes = new HashSet<>();
    pinnedInodes.add(mRandom.nextLong());

    mBlockWorker.updatePinList(pinnedInodes);
    verify(mBlockStore).updatePinnedInodes(pinnedInodes);
  }

  /**
   * Tests the {@link BlockWorker#getFileInfo(long)} method.
   */
  @Test
  public void getFileInfo() throws Exception {
    long fileId = mRandom.nextLong();

    mBlockWorker.getFileInfo(fileId);
    verify(mFileSystemMasterClient).getFileInfo(fileId);
  }
}
