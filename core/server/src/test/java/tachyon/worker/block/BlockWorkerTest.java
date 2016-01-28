/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

import tachyon.Sessions;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.worker.DataServer;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.TempBlockMeta;
import tachyon.worker.file.FileSystemMasterClient;

/**
 * Unit tests for {@link BlockDataManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, FileSystemMasterClient.class,
    BlockHeartbeatReporter.class, BlockMetricsReporter.class, BlockMeta.class,
    BlockStoreLocation.class, BlockStoreMeta.class, StorageDir.class, TachyonConf.class,
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
   * @throws IOException if initialize fails
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

    TachyonConf conf = WorkerContext.getConf();
    conf.set("tachyon.worker.tieredstore.level0.dirs.path",
        mFolder.newFolder().getAbsolutePath());
    mBlockWorker = new BlockWorker();

    Whitebox.setInternalState(mBlockWorker, "mBlockMasterClient", mBlockMasterClient);
    Whitebox.setInternalState(mBlockWorker, "mFileSystemMasterClient", mFileSystemMasterClient);
    Whitebox.setInternalState(mBlockWorker, "mBlockStore", mBlockStore);
    Whitebox.setInternalState(mBlockWorker, "mHeartbeatReporter", mHeartbeatReporter);
    Whitebox.setInternalState(mBlockWorker, "mMetricsReporter", mMetricsReporter);
    Whitebox.setInternalState(mBlockWorker, "mSessions", mSessions);
  }

  /**
   * Stop the DataServer to clean up
   *
   * @throws IOException if clean up fails
   */
  @After
  public void after() throws IOException {
    ((DataServer) Whitebox.getInternalState(mBlockWorker, "mDataServer")).close();
  }
  /**
   * Tests the {@link BlockDataManager#abortBlock(long, long)} method.
   *
   * @throws Exception if aborting the block fails
   */
  @Test
  public void abortBlockTest() throws Exception {
    mBlockWorker.abortBlock(anyLong(), anyLong());
    verify(mBlockStore).abortBlock(anyLong(), anyLong());
  }

  /**
   * Tests the {@link BlockDataManager#accessBlock(long, long)} method.
   *
   * @throws Exception if accessing the block fails
   */
  @Test
  public void accessBlockTest() throws Exception {
    mBlockWorker.accessBlock(anyLong(), anyLong());
    verify(mBlockStore).accessBlock(anyLong(), anyLong());
  }

  /**
   * Tests the {@link BlockDataManager#cleanupSessions()} method.
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
   * Tests the {@link BlockDataManager#commitBlock(long, long)} method.
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
   * Tests the {@link BlockDataManager#createBlock(long, long, String, long)} method.
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

}
