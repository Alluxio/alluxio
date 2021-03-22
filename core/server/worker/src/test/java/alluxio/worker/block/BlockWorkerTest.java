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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.ServerConfiguration;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.Sessions;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.grpc.ReadRequest;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.DefaultBlockMeta;
import alluxio.worker.block.meta.DefaultTempBlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Unit tests for {@link DefaultBlockWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, BlockMasterClientPool.class, FileSystemMasterClient.class,
    BlockHeartbeatReporter.class, BlockMetricsReporter.class,
    BlockStoreLocation.class, ServerConfiguration.class, UnderFileSystem.class,
    BlockWorker.class, Sessions.class})
public class BlockWorkerTest {

  private BlockMasterClient mBlockMasterClient;
  private BlockMasterClientPool mBlockMasterClientPool;
  private BlockStore mBlockStore;
  private BlockStoreMeta mBlockStoreMeta;
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
          .put(PropertyKey.WORKER_RPC_PORT, Integer.toString(0))
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS, "HDD")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, AlluxioTestDirectory
              .createTemporaryDirectory("WORKER_TIERED_STORE_LEVEL1_DIRS_PATH").getAbsolutePath())
          .build(), ServerConfiguration.global());

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws IOException {
    mRandom = new Random();
    mBlockMasterClient = PowerMockito.mock(BlockMasterClient.class);
    mBlockMasterClientPool = spy(new BlockMasterClientPool());
    when(mBlockMasterClientPool.createNewResource()).thenReturn(mBlockMasterClient);
    mBlockStore = PowerMockito.mock(BlockStore.class);
    mBlockStoreMeta = mock(BlockStoreMeta.class);
    when(mBlockStore.getBlockStoreMeta()).thenReturn(mBlockStoreMeta);
    when(mBlockStoreMeta.getStorageTierAssoc()).thenReturn(new WorkerStorageTierAssoc());
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    mSessions = PowerMockito.mock(Sessions.class);
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
   * Tests the {@link BlockWorker#commitBlock(long, long, boolean)} method.
   */
  @Test
  public void commitBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long length = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long usedBytes = mRandom.nextLong();
    String tierAlias = "MEM";
    String mediumType = "MEM";
    HashMap<String, Long> usedBytesOnTiers = new HashMap<>();
    usedBytesOnTiers.put(tierAlias, usedBytes);
    BlockMeta blockMeta = mock(BlockMeta.class);
    BlockStoreLocation blockStoreLocation = PowerMockito.mock(BlockStoreLocation.class);
    BlockStoreMeta blockStoreMeta = mock(BlockStoreMeta.class);

    when(mBlockStore.lockBlock(sessionId, blockId)).thenReturn(lockId);
    when(mBlockStore.getBlockMeta(eq(sessionId), eq(blockId), anyLong())).thenReturn(blockMeta);
    when(mBlockStore.getBlockStoreMeta()).thenReturn(blockStoreMeta);
    when(mBlockStore.getBlockStoreMetaFull()).thenReturn(blockStoreMeta);

    when(blockMeta.getBlockLocation()).thenReturn(blockStoreLocation);
    when(blockStoreLocation.tierAlias()).thenReturn(tierAlias);
    when(blockStoreLocation.mediumType()).thenReturn(mediumType);
    when(blockMeta.getBlockSize()).thenReturn(length);
    when(blockStoreMeta.getUsedBytesOnTiers()).thenReturn(usedBytesOnTiers);

    mBlockWorker.commitBlock(sessionId, blockId, mRandom.nextBoolean());
    verify(mBlockMasterClient).commitBlock(anyLong(), eq(usedBytes), eq(tierAlias), eq(mediumType),
        eq(blockId), eq(length));
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
    BlockMeta blockMeta = mock(BlockMeta.class);
    BlockStoreLocation blockStoreLocation = PowerMockito.mock(BlockStoreLocation.class);
    BlockStoreMeta blockStoreMeta = mock(BlockStoreMeta.class);

    when(mBlockStore.lockBlock(sessionId, blockId)).thenReturn(lockId);
    when(mBlockStore.getBlockMeta(eq(sessionId), eq(blockId), anyLong())).thenReturn(blockMeta);
    when(mBlockStore.getBlockStoreMeta()).thenReturn(blockStoreMeta);
    when(blockMeta.getBlockLocation()).thenReturn(blockStoreLocation);
    when(blockStoreLocation.tierAlias()).thenReturn(tierAlias);
    when(blockMeta.getBlockSize()).thenReturn(length);
    when(blockStoreMeta.getUsedBytesOnTiers()).thenReturn(usedBytesOnTiers);

    doThrow(new BlockAlreadyExistsException("")).when(mBlockStore).commitBlock(sessionId, blockId,
        false);
    mBlockWorker.commitBlock(sessionId, blockId, mRandom.nextBoolean());
  }

  /**
   * Tests the {@link BlockWorker#createBlock)} method.
   */
  @Test
  public void createBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = mock(StorageDir.class);
    TempBlockMeta meta = new DefaultTempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlock(sessionId, blockId,
        AllocateOptions.forCreate(initialBytes, location))).thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(
        PathUtils.concatPath("/tmp", ".tmp_blocks", sessionId % 1024,
            String.format("%x-%x", sessionId, blockId)),
        mBlockWorker.createBlock(sessionId, blockId, 0, "", initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#createBlock} method with
   * a tier other than MEM.
   */
  @Test
  public void createBlockLowerTier() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "HDD";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = mock(StorageDir.class);
    TempBlockMeta meta = new DefaultTempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlock(sessionId, blockId,
        AllocateOptions.forCreate(initialBytes, location))).thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(
        PathUtils.concatPath("/tmp", ".tmp_blocks", sessionId % 1024,
            String.format("%x-%x", sessionId, blockId)),
        mBlockWorker.createBlock(sessionId, blockId, 1, "", initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#createBlock} method.
   */
  @Test
  public void createBlockRemote() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = mock(StorageDir.class);
    TempBlockMeta meta = new DefaultTempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlock(sessionId, blockId,
        AllocateOptions.forCreate(initialBytes, location))).thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(
        PathUtils.concatPath("/tmp", ".tmp_blocks", sessionId % 1024,
            String.format("%x-%x", sessionId, blockId)),
        mBlockWorker.createBlock(sessionId, blockId, 0, "", initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#getBlockWriter(long, long)} method.
   */
  @Test
  public void getTempBlockWriterRemote() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.getBlockWriter(sessionId, blockId);
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
    verify(mBlockStore, times(2)).getBlockStoreMeta(); // 1 is called at metrics registration
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
    verify(mBlockStore).lockBlockNoException(sessionId, blockId);
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
    BlockStoreLocation existingLocation = mock(BlockStoreLocation.class);
    when(existingLocation.belongsTo(location)).thenReturn(false);
    BlockMeta meta = mock(BlockMeta.class);
    when(meta.getBlockLocation()).thenReturn(existingLocation);
    when(mBlockStore.getBlockMeta(eq(sessionId), eq(blockId), anyLong()))
        .thenReturn(meta);
    mBlockWorker.moveBlock(sessionId, blockId, tierAlias);
    verify(mBlockStore).moveBlock(sessionId, blockId,
        AllocateOptions.forMove(location).setSize(meta.getBlockSize()));
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
    BlockStoreLocation existingLocation = mock(BlockStoreLocation.class);
    when(existingLocation.belongsTo(location)).thenReturn(true);
    BlockMeta meta = mock(BlockMeta.class);
    when(meta.getBlockLocation()).thenReturn(existingLocation);
    when(mBlockStore.getBlockMeta(eq(sessionId), eq(blockId), anyLong()))
        .thenReturn(meta);
    mBlockWorker.moveBlock(sessionId, blockId, tierAlias);
    verify(mBlockStore, times(0)).moveBlock(sessionId, blockId,
        AllocateOptions.forMove(location).setSize(meta.getBlockSize()));
  }

  /**
   * Tests the {@link BlockWorker#getLocalBlockPath(long, long, long)} method.
   */
  @Test
  public void readBlock() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    long blockSize = mRandom.nextLong();
    StorageDir storageDir = mock(StorageDir.class);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    BlockMeta meta = new DefaultBlockMeta(blockId, blockSize, storageDir);
    when(mBlockStore.getBlockMeta(sessionId, blockId, lockId)).thenReturn(meta);

    mBlockWorker.getLocalBlockPath(sessionId, blockId, lockId);
    verify(mBlockStore).getBlockMeta(sessionId, blockId, lockId);
    assertEquals(PathUtils.concatPath("/tmp", blockId),
        mBlockWorker.getLocalBlockPath(sessionId, blockId, lockId));
  }

  /**
   * Tests the {@link BlockWorker#newLocalBlockReader(long, long, long)} method.
   */
  @Test
  public void readBlockRemote() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long lockId = mRandom.nextLong();

    mBlockWorker.newLocalBlockReader(sessionId, blockId, lockId);
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

  @Test
  public void getAndCleanBlockReader() throws Exception {
    long blockId = mRandom.nextLong();
    BlockReadRequest request = new BlockReadRequest(
        ReadRequest.newBuilder().setBlockId(blockId).setOffset(0).setLength(10).build());
    long sessionId = request.getSessionId();

    // Create a real file and block reader to avoid NPE
    BlockReader blockReader = prepareBlockReader();
    doReturn(blockReader).when(mBlockStore).getBlockReader(anyLong(), anyLong(), anyLong());

    Assert.assertEquals(blockReader, mBlockWorker.newBlockReader(request));
    verify(mBlockStore).lockBlockNoException(sessionId, blockId);
    verify(mBlockStore).accessBlock(sessionId, blockId);

    mBlockWorker.closeBlockReader(blockReader, request);
    verify(mBlockStore).unlockBlock(sessionId, blockId);
  }

  /**
   * Creates a real file and a block reader.
   *
   * @return the block reader
   */
  protected BlockReader prepareBlockReader() throws Exception {
    File file = mTestFolder.newFile();
    int chunkSize = 10;
    int length = chunkSize * 10;
    if (length > 0) {
      FileOutputStream fileOutputStream = new FileOutputStream(file);
      while (length > 0) {
        byte[] buffer = new byte[(int) Math.min(length, Constants.MB)];
        mRandom.nextBytes(buffer);
        fileOutputStream.write(buffer);
        length -= buffer.length;
      }
      fileOutputStream.close();
    }
    BlockReader blockReader = new LocalFileBlockReader(file.getPath());
    return blockReader;
  }
}
