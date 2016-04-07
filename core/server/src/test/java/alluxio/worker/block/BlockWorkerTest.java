/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerContext;
import alluxio.worker.WorkerIdRegistry;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link BlockWorker}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, FileSystemMasterClient.class,
    BlockHeartbeatReporter.class, BlockMetricsReporter.class, BlockMeta.class,
    BlockStoreLocation.class, BlockStoreMeta.class, StorageDir.class, Configuration.class,
    UnderFileSystem.class, BlockWorker.class})
public class BlockWorkerTest {

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private BlockMasterClient mBlockMasterClient;
  private BlockStore mBlockStore;
  private FileSystemMasterClient mFileSystemMasterClient;
  private BlockHeartbeatReporter mHeartbeatReporter;
  private BlockMetricsReporter mMetricsReporter;
  private Random mRandom;
  private Sessions mSessions;
  private long mWorkerId;
  private BlockWorker mBlockWorker;

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws IOException if initialization fails
   */
  @Before
  public void before() throws IOException {
    mRandom = new Random();

    mBlockMasterClient = PowerMockito.mock(BlockMasterClient.class);
    mBlockStore = PowerMockito.mock(BlockStore.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    mHeartbeatReporter = PowerMockito.mock(BlockHeartbeatReporter.class);
    mMetricsReporter = PowerMockito.mock(BlockMetricsReporter.class);
    mSessions = PowerMockito.mock(Sessions.class);
    mWorkerId = mRandom.nextLong();
    ((AtomicLong) Whitebox.getInternalState(WorkerIdRegistry.class, "sWorkerId")).set(mWorkerId);

    Configuration conf = WorkerContext.getConf();
    conf.set("alluxio.worker.tieredstore.level0.dirs.path",
        mFolder.newFolder().getAbsolutePath());
    conf.set(Constants.WORKER_DATA_PORT, Integer.toString(0));

    mBlockWorker = new BlockWorker();

    Whitebox.setInternalState(mBlockWorker, "mBlockMasterClient", mBlockMasterClient);
    Whitebox.setInternalState(mBlockWorker, "mFileSystemMasterClient", mFileSystemMasterClient);
    Whitebox.setInternalState(mBlockWorker, "mBlockStore", mBlockStore);
    Whitebox.setInternalState(mBlockWorker, "mHeartbeatReporter", mHeartbeatReporter);
    Whitebox.setInternalState(mBlockWorker, "mMetricsReporter", mMetricsReporter);
    Whitebox.setInternalState(mBlockWorker, "mSessions", mSessions);
  }

  /**
   * Resets the worker context.
   */
  @After
  public void after() throws IOException {
    WorkerContext.reset();
  }

  /**
   * Tests the {@link BlockWorker#abortBlock(long, long)} method.
   *
   * @throws Exception if aborting the block fails
   */
  @Test
  public void abortBlockTest() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.abortBlock(sessionId, blockId);
    verify(mBlockStore).abortBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#accessBlock(long, long)} method.
   *
   * @throws Exception if accessing the block fails
   */
  @Test
  public void accessBlockTest() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.accessBlock(sessionId, blockId);
    verify(mBlockStore).accessBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#cleanupSessions()} method.
   */
  @Test
  public void cleanupSessionsTest() {
    long sessionId = 1;
    LinkedList<Long> sessions = new LinkedList<Long>();
    sessions.add(sessionId);

    when(mSessions.getTimedOutSessions()).thenReturn(sessions);
    mBlockWorker.cleanupSessions();
    verify(mSessions).removeSession(sessionId);
    verify(mBlockStore).cleanupSession(sessionId);
  }

  /**
   * Tests the {@link BlockWorker#commitBlock(long, long)} method.
   *
   * @throws Exception if a block operation fails
   */
  @Test
  public void commitBlockTest() throws Exception {
    long blockId = mRandom.nextLong();
    long length = mRandom.nextLong();
    long lockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long usedBytes = mRandom.nextLong();
    String tierAlias = "MEM";
    HashMap<String, Long> usedBytesOnTiers = new HashMap<String, Long>();
    usedBytesOnTiers.put(tierAlias, usedBytes);
    BlockMeta blockMeta = PowerMockito.mock(BlockMeta.class);
    BlockStoreLocation blockStoreLocation = PowerMockito.mock(BlockStoreLocation.class);
    BlockStoreMeta blockStoreMeta = PowerMockito.mock(BlockStoreMeta.class);

    when(mBlockStore.lockBlock(sessionId, blockId)).thenReturn(lockId);
    when(mBlockStore.getBlockMeta(sessionId, blockId, lockId)).thenReturn(
        blockMeta);
    when(mBlockStore.getBlockStoreMeta()).thenReturn(blockStoreMeta);
    when(blockMeta.getBlockLocation()).thenReturn(blockStoreLocation);
    when(blockStoreLocation.tierAlias()).thenReturn(tierAlias);
    when(blockMeta.getBlockSize()).thenReturn(length);
    when(blockStoreMeta.getUsedBytesOnTiers()).thenReturn(usedBytesOnTiers);

    mBlockWorker.commitBlock(sessionId, blockId);
    verify(mBlockMasterClient).commitBlock(mWorkerId, usedBytes,
        tierAlias, blockId, length);
    verify(mBlockStore).unlockBlock(lockId);
  }

  /**
   * Tests the {@link BlockWorker#createBlock(long, long, String, long)} method.
   *
   * @throws Exception if the creation of the block or its metadata fails
   */
  @Test
  public void createBlockTest() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    TempBlockMeta meta = new TempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlockMeta(sessionId, blockId, location, initialBytes))
        .thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(PathUtils.concatPath("/tmp", sessionId, blockId),
        mBlockWorker.createBlock(sessionId, blockId, tierAlias, initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#createBlockRemote(long, long, String, long)} method.
   *
   * @throws Exception if the creation of the block or its metadata fails
   */
  @Test
  public void createBlockRemoteTest() throws Exception {
    long blockId = mRandom.nextLong();
    long initialBytes = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    TempBlockMeta meta = new TempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    when(mBlockStore.createBlockMeta(sessionId, blockId, location, initialBytes))
        .thenReturn(meta);
    when(storageDir.getDirPath()).thenReturn("/tmp");
    assertEquals(PathUtils.concatPath("/tmp", sessionId, blockId),
        mBlockWorker.createBlock(sessionId, blockId, tierAlias, initialBytes));
  }

  /**
   * Tests the {@link BlockWorker#freeSpace(long, long, String)} method.
   *
   * @throws Exception if the free space check fails
   */
  @Test
  public void freeSpaceTest() throws Exception {
    long sessionId = mRandom.nextLong();
    long availableBytes = mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockWorker.freeSpace(sessionId, availableBytes, tierAlias);
    verify(mBlockStore).freeSpace(sessionId, availableBytes, location);
  }

  /**
   * Tests the {@link BlockWorker#getTempBlockWriterRemote(long, long)} method.
   *
   * @throws Exception if the method check fails
   */
  @Test
  public void getTempBlockWriterRemoteTest() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.getTempBlockWriterRemote(sessionId, blockId);
    verify(mBlockStore).getBlockWriter(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#getReport()} method.
   *
   */
  @Test
  public void getReportTest() {
    mBlockWorker.getReport();
    verify(mHeartbeatReporter).generateReport();
  }

  /**
   * Tests the {@link BlockWorker#getStoreMeta()} method.
   *
   */
  @Test
  public void getStoreMetaTest() {
    mBlockWorker.getStoreMeta();
    verify(mBlockStore).getBlockStoreMeta();
  }

  /**
   * Tests the {@link BlockWorker#getVolatileBlockMeta(long)} method.
   *
   * @throws Exception if the getVolatileBlockMeta check fails
   */
  @Test
  public void getVolatileBlockMetaTest() throws Exception {
    long blockId = mRandom.nextLong();
    mBlockWorker.getVolatileBlockMeta(blockId);
    verify(mBlockStore).getVolatileBlockMeta(blockId);
  }

  /**
   * Tests the {@link BlockWorker#hasBlockMeta(long)} method.
   *
   */
  @Test
  public void hasBlockMetaTest() {
    long blockId = mRandom.nextLong();
    mBlockWorker.hasBlockMeta(blockId);
    verify(mBlockStore).hasBlockMeta(blockId);
  }

  /**
   * Tests the {@link BlockWorker#lockBlock(long, long)} method.
   *
   * @throws Exception if the lockBlock check fails
   */
  @Test
  public void lockBlockTest() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.lockBlock(sessionId, blockId);
    verify(mBlockStore).lockBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#moveBlock(long, long, String)} method.
   *
   * @throws Exception if the moveBlock check fails
   */
  @Test
  public void moveBlockTest() throws Exception {
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
   *
   * @throws Exception if the moveBlock check fails
   */
  @Test
  public void moveBlockNoopTest() throws Exception {
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
   *
   * @throws Exception if the readBlock check fails
   */
  @Test
  public void readBlockTest() throws Exception {
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
   *
   * @throws Exception if the readBlockRemote check fails
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
   *
   * @throws Exception if the removeBlock check fails
   */
  @Test
  public void removeBlockTest() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    mBlockWorker.removeBlock(sessionId, blockId);
    verify(mBlockStore).removeBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#requestSpace(long, long, long)} method.
   *
   * @throws Exception if the requestSpace check fails
   */
  @Test
  public void requestSpaceTest() throws Exception {
    long blockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long additionalBytes = mRandom.nextLong();
    mBlockWorker.requestSpace(sessionId, blockId, additionalBytes);
    verify(mBlockStore).requestSpace(sessionId, blockId, additionalBytes);
  }

  /**
   * Tests the {@link BlockWorker#unlockBlock(long)} and
   * {@link BlockWorker#unlockBlock(long, long)} method.
   *
   * @throws Exception if the unlockBlock check fails
   */
  @Test
  public void unlockBlockTest() throws Exception {
    long lockId = mRandom.nextLong();
    long sessionId = mRandom.nextLong();
    long blockId = mRandom.nextLong();

    mBlockWorker.unlockBlock(lockId);
    verify(mBlockStore).unlockBlock(lockId);

    mBlockWorker.unlockBlock(sessionId, blockId);
    verify(mBlockStore).unlockBlock(sessionId, blockId);
  }

  /**
   * Tests the {@link BlockWorker#sessionHeartbeat(long, List<long>)} method.
   *
   */
  @Test
  public void sessionHeartbeatTest() {
    long sessionId = mRandom.nextLong();
    List<Long> metrics = new ArrayList<Long>();
    metrics.add(mRandom.nextLong());

    mBlockWorker.sessionHeartbeat(sessionId, metrics);
    verify(mSessions).sessionHeartbeat(sessionId);
    verify(mMetricsReporter).updateClientMetrics(metrics);
  }

  /**
   * Tests the {@link BlockWorker#updatePinList(Set<long>)} method.
   *
   */
  @Test
  public void updatePinListTest() {
    Set<Long> pinnedInodes = new HashSet<Long>();
    pinnedInodes.add(mRandom.nextLong());

    mBlockWorker.updatePinList(pinnedInodes);
    verify(mBlockStore).updatePinnedInodes(pinnedInodes);
  }

  /**
   * Tests the {@link BlockWorker#getFileInfo(long)} method.
   *
   * @throws Exception if the getFileInfo check fails
   */
  @Test
  public void getFileInfoTest() throws Exception {
    long fileId = mRandom.nextLong();

    mBlockWorker.getFileInfo(fileId);
    verify(mFileSystemMasterClient).getFileInfo(fileId);
  }
}
