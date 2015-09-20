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
import java.util.LinkedList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.powermock.reflect.Whitebox;
import tachyon.Constants;
import tachyon.Sessions;
import tachyon.client.WorkerBlockMasterClient;
import tachyon.client.WorkerFileSystemMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.test.Tester;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.TempBlockMeta;

@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerBlockMasterClient.class, WorkerFileSystemMasterClient.class,
    BlockHeartbeatReporter.class, BlockMetricsReporter.class, BlockMeta.class,
    BlockStoreLocation.class, BlockStoreMeta.class, StorageDir.class, TachyonConf.class,
    WorkerContext.class})
public class BlockDataManagerTest implements Tester<BlockDataManager> {
  private TestHarness mHarness;
  private BlockDataManager.PrivateAccess mPrivateAccess;

  @Override
  public void receiveAccess(Object access) {
    mPrivateAccess = (BlockDataManager.PrivateAccess) access;
  }

  private class TestHarness {
    WorkerBlockMasterClient mBlockMasterClient;
    BlockStore mBlockStore;
    WorkerFileSystemMasterClient mFileSystemMasterClient;
    BlockHeartbeatReporter mHeartbeatReporter;
    BlockDataManager mManager;
    BlockMetricsReporter mMetricsReporter;
    Random mRandom;
    Sessions mSessions;
    TachyonConf mTachyonConf;
    UnderFileSystem mUfs;
    long mWorkerId;
    WorkerSource mWorkerSource;

    public TestHarness() throws IOException {
      mRandom = new Random();

      mBlockMasterClient = PowerMockito.mock(WorkerBlockMasterClient.class);
      mBlockStore = PowerMockito.mock(BlockStore.class);
      mFileSystemMasterClient = PowerMockito.mock(WorkerFileSystemMasterClient.class);
      mHeartbeatReporter = PowerMockito.mock(BlockHeartbeatReporter.class);
      mMetricsReporter = PowerMockito.mock(BlockMetricsReporter.class);
      mSessions = PowerMockito.mock(Sessions.class);
      mTachyonConf = PowerMockito.mock(TachyonConf.class);
      mUfs = PowerMockito.mock(UnderFileSystem.class);
      mWorkerId = mRandom.nextLong();
      mWorkerSource = PowerMockito.mock(WorkerSource.class);

      TachyonConf conf = PowerMockito.mock(TachyonConf.class);
      Mockito.when(conf.get(Constants.UNDERFS_ADDRESS)).thenReturn("/tmp");
      Mockito.when(conf.get(Constants.UNDERFS_DATA_FOLDER)).thenReturn("/tmp");
      Whitebox.setInternalState(WorkerContext.class, "sTachyonConf", conf);

      mManager =
          new BlockDataManager(mWorkerSource, mBlockMasterClient, mFileSystemMasterClient,
              mBlockStore);
      mManager.setSessions(mSessions);
      mManager.setWorkerId(mWorkerId);

      mManager.grantAccess(BlockDataManagerTest.this); // initializes mPrivateAccess
      mPrivateAccess.setHeartbeatReporter(mHeartbeatReporter);
      mPrivateAccess.setMetricsReporter(mMetricsReporter);
      mPrivateAccess.setUnderFileSystem(mUfs);
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
  public void addCheckpointTest() throws Exception {
    long fileId = mHarness.mRandom.nextLong();
    long fileSize = mHarness.mRandom.nextLong();
    long sessionId = mHarness.mRandom.nextLong();
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("/foo/bar");
    String srcPath = "/tmp/" + fileId;
    String parentPath = "/tmp/foo";
    String dstPath = "/tmp/foo/bar";

    // TODO(jiri): Add test cases for error cases.
    Mockito.when(mHarness.mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER)).thenReturn("/tmp");
    Mockito.when(mHarness.mSessions.getSessionUfsTempFolder(sessionId)).thenReturn("/tmp");
    Mockito.when(mHarness.mUfs.exists(parentPath)).thenReturn(true);
    Mockito.when(mHarness.mUfs.mkdirs(parentPath, true)).thenReturn(true);
    Mockito.when(mHarness.mUfs.rename(srcPath, dstPath)).thenReturn(true);
    Mockito.when(mHarness.mUfs.getFileSize(dstPath)).thenReturn(fileSize);
    Mockito.when(mHarness.mFileSystemMasterClient.getFileInfo(fileId)).thenReturn(fileInfo);
    mHarness.mManager.addCheckpoint(sessionId, fileId);
    Mockito.verify(mHarness.mFileSystemMasterClient).addCheckpoint(mHarness.mWorkerId, fileId,
        fileSize, dstPath);
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
    int tierAlias = 1;
    LinkedList<Long> usedBytesOnTiers = new LinkedList<Long>();
    usedBytesOnTiers.add(usedBytes);
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
    int tierAlias = 1;
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
