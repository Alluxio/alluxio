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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import tachyon.Sessions;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.WorkerSource;
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
    UnderFileSystem.class})
public class BlockDataManagerTest {
  private TestHarness mHarness;

  private class TestHarness {
    BlockMasterClient mBlockMasterClient;
    BlockStore mBlockStore;
    FileSystemMasterClient mFileSystemMasterClient;
    BlockHeartbeatReporter mHeartbeatReporter;
    BlockDataManager mManager;
    BlockMetricsReporter mMetricsReporter;
    Random mRandom;
    Sessions mSessions;
    long mWorkerId;
    WorkerSource mWorkerSource;

    public TestHarness() throws IOException {
      mRandom = new Random();

      mBlockMasterClient = PowerMockito.mock(BlockMasterClient.class);
      mBlockStore = PowerMockito.mock(BlockStore.class);
      mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
      mHeartbeatReporter = PowerMockito.mock(BlockHeartbeatReporter.class);
      mMetricsReporter = PowerMockito.mock(BlockMetricsReporter.class);
      mSessions = PowerMockito.mock(Sessions.class);
      mWorkerId = mRandom.nextLong();
      ((AtomicLong) Whitebox.getInternalState(WorkerIdRegistry.class, "sWorkerId")).set(mWorkerId);
      mWorkerSource = PowerMockito.mock(WorkerSource.class);

      mManager =
          new BlockDataManager(mWorkerSource, mBlockMasterClient, mFileSystemMasterClient,
              mBlockStore);

      Whitebox.setInternalState(mManager, "mHeartbeatReporter", mHeartbeatReporter);
      Whitebox.setInternalState(mManager, "mMetricsReporter", mMetricsReporter);
      Whitebox.setInternalState(mManager, "mSessions", mSessions);
    }
  }

  @Before
  public void initialize() throws IOException {
    mHarness = new TestHarness();
  }

  @Test
  public void abortBlockTest() throws Exception {
    long blockId = mHarness.mRandom.nextLong();
    long sessionId = mHarness.mRandom.nextLong();
    mHarness.mManager.abortBlock(sessionId, blockId);
    Mockito.verify(mHarness.mBlockStore).abortBlock(sessionId, blockId);
  }

  @Test
  public void accessBlockTest() throws Exception {
    long blockId = mHarness.mRandom.nextLong();
    long sessionId = mHarness.mRandom.nextLong();
    mHarness.mManager.accessBlock(sessionId, blockId);
    Mockito.verify(mHarness.mBlockStore).accessBlock(sessionId, blockId);
  }

  @Test
  public void cleanupSessionsTest() throws Exception {
    long sessionId = 1;
    LinkedList<Long> sessions = new LinkedList<Long>();
    sessions.add(sessionId);

    Mockito.when(mHarness.mSessions.getTimedOutSessions()).thenReturn(sessions);
    mHarness.mManager.cleanupSessions();
    Mockito.verify(mHarness.mSessions).removeSession(sessionId);
    Mockito.verify(mHarness.mBlockStore).cleanupSession(sessionId);
  }

  @Test
  public void commitBlockTest() throws Exception {
    long blockId = mHarness.mRandom.nextLong();
    long length = mHarness.mRandom.nextLong();
    long lockId = mHarness.mRandom.nextLong();
    long sessionId = mHarness.mRandom.nextLong();
    long usedBytes = mHarness.mRandom.nextLong();
    String tierAlias = "MEM";
    HashMap<String, Long> usedBytesOnTiers = new HashMap<String, Long>();
    usedBytesOnTiers.put(tierAlias, usedBytes);
    BlockMeta blockMeta = PowerMockito.mock(BlockMeta.class);
    BlockStoreLocation blockStoreLocation = PowerMockito.mock(BlockStoreLocation.class);
    BlockStoreMeta blockStoreMeta = PowerMockito.mock(BlockStoreMeta.class);

    Mockito.when(mHarness.mBlockStore.lockBlock(sessionId, blockId)).thenReturn(lockId);
    Mockito.when(mHarness.mBlockStore.getBlockMeta(sessionId, blockId, lockId)).thenReturn(
        blockMeta);
    Mockito.when(mHarness.mBlockStore.getBlockStoreMeta()).thenReturn(blockStoreMeta);
    Mockito.when(blockMeta.getBlockLocation()).thenReturn(blockStoreLocation);
    Mockito.when(blockStoreLocation.tierAlias()).thenReturn(tierAlias);
    Mockito.when(blockMeta.getBlockSize()).thenReturn(length);
    Mockito.when(blockStoreMeta.getUsedBytesOnTiers()).thenReturn(usedBytesOnTiers);

    mHarness.mManager.commitBlock(sessionId, blockId);
    Mockito.verify(mHarness.mBlockMasterClient).commitBlock(mHarness.mWorkerId, usedBytes,
        tierAlias, blockId, length);
    Mockito.verify(mHarness.mBlockStore).unlockBlock(lockId);
  }

  @Test
  public void createBlockTest() throws Exception {
    long blockId = mHarness.mRandom.nextLong();
    long initialBytes = mHarness.mRandom.nextLong();
    long sessionId = mHarness.mRandom.nextLong();
    String tierAlias = "MEM";
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    TempBlockMeta meta = new TempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    Mockito.when(mHarness.mBlockStore.createBlockMeta(sessionId, blockId, location, initialBytes))
        .thenReturn(meta);
    Mockito.when(storageDir.getDirPath()).thenReturn("/tmp");
    Assert.assertEquals(PathUtils.concatPath("/tmp", sessionId, blockId),
        mHarness.mManager.createBlock(sessionId, blockId, tierAlias, initialBytes));
  }

  // TODO(jiri): Write unit tests for untested public methods.
}
