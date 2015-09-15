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
import org.junit.Test;
import org.mockito.Mockito;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.client.BlockMasterClient;
import tachyon.client.FileSystemMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.test.Tester;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.TempBlockMeta;

public class BlockDataManagerTest {
  class BlockDataManagerTester implements Tester<BlockDataManager> {
    BlockDataManager.PrivateAccess mPrivateAccess;

    public void receiveAccess(Object access) {
      mPrivateAccess = (BlockDataManager.PrivateAccess) access;
    }
  }

  class TestHarness {
    BlockMasterClient mBlockMasterClient;
    BlockStore mBlockStore;
    FileSystemMasterClient mFileSystemMasterClient;
    BlockHeartbeatReporter mHeartbeatReporter;
    BlockDataManager mManager;
    BlockMetricsReporter mMetricsReporter;
    Random mRandom;
    Sessions mSessions;
    TachyonConf mTachyonConf;
    BlockDataManagerTester mTester;
    UnderFileSystem mUfs;
    long mWorkerId;
    WorkerSource mWorkerSource;

    public TestHarness() throws IOException {
      mRandom = new Random();

      mBlockMasterClient = Mockito.mock(BlockMasterClient.class);
      mBlockStore = Mockito.mock(BlockStore.class);
      mFileSystemMasterClient = Mockito.mock(FileSystemMasterClient.class);
      mHeartbeatReporter = Mockito.mock(BlockHeartbeatReporter.class);
      mMetricsReporter = Mockito.mock(BlockMetricsReporter.class);
      mSessions = Mockito.mock(Sessions.class);
      mTachyonConf = Mockito.mock(TachyonConf.class);
      mUfs = Mockito.mock(UnderFileSystem.class);
      mWorkerId = mRandom.nextLong();
      mWorkerSource = Mockito.mock(WorkerSource.class);

      mManager = new BlockDataManager(mWorkerSource, mBlockMasterClient, mFileSystemMasterClient);
      mManager.setSessions(mSessions);
      mManager.setWorkerId(mWorkerId);

      mTester = new BlockDataManagerTester();
      mManager.grantAccess(mTester);
      mTester.mPrivateAccess.setBlockStore(mBlockStore);
      mTester.mPrivateAccess.setHeartbeatReporter(mHeartbeatReporter);
      mTester.mPrivateAccess.setMetricsReporter(mMetricsReporter);
      mTester.mPrivateAccess.setTachyonConf(mTachyonConf);
      mTester.mPrivateAccess.setUnderFileSystem(mUfs);
    }
  }

  @Test
  public void AbortBlockTest() throws Exception {
    TestHarness harness = new TestHarness();
    long blockId = harness.mRandom.nextLong();
    long sessionId = harness.mRandom.nextLong();
    harness.mManager.abortBlock(sessionId, blockId);
    Mockito.verify(harness.mBlockStore).abortBlock(sessionId, blockId);
  }

  @Test
  public void AccessBlockTest() throws Exception {
    TestHarness harness = new TestHarness();
    long blockId = harness.mRandom.nextLong();
    long sessionId = harness.mRandom.nextLong();
    harness.mManager.accessBlock(sessionId, blockId);
    Mockito.verify(harness.mBlockStore).accessBlock(sessionId, blockId);
  }

  @Test
  public void AddCheckpointTest() throws Exception {
    TestHarness harness = new TestHarness();
    long fileId = harness.mRandom.nextLong();
    long fileSize = harness.mRandom.nextLong();
    long sessionId = harness.mRandom.nextLong();
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("/foo/bar");
    String srcPath = "/tmp/" + fileId;
    String parentPath = "/tmp/foo";
    String dstPath = "/tmp/foo/bar";

    // TODO(jsimsa): Add test cases for error cases.
    Mockito.when(harness.mTachyonConf
        .get(Constants.UNDERFS_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER)).thenReturn("/tmp");
    Mockito.when(harness.mSessions.getSessionUfsTempFolder(sessionId)).thenReturn("/tmp");
    Mockito.when(harness.mFileSystemMasterClient.getFileInfo(fileId)).thenReturn(fileInfo);
    Mockito.when(harness.mUfs.exists(parentPath)).thenReturn(true);
    Mockito.when(harness.mUfs.mkdirs(parentPath, true)).thenReturn(true);
    Mockito.when(harness.mUfs.rename(srcPath, dstPath)).thenReturn(true);
    Mockito.when(harness.mUfs.getFileSize(dstPath)).thenReturn(fileSize);
    harness.mManager.addCheckpoint(sessionId, fileId);
    Mockito.verify(harness.mFileSystemMasterClient)
        .addCheckpoint(harness.mWorkerId, fileId, fileSize, dstPath);
  }

  @Test
  public void CleanupSessionsTest() throws Exception {
    TestHarness harness = new TestHarness();
    long sessionId = 1;
    LinkedList<Long> sessions = new LinkedList<Long>();
    sessions.add(sessionId);

    Mockito.when(harness.mSessions.getTimedOutSessions()).thenReturn(sessions);
    harness.mManager.cleanupSessions();
    Mockito.verify(harness.mSessions).removeSession(sessionId);
    Mockito.verify(harness.mBlockStore).cleanupSession(sessionId);
  }

  @Test
  public void CommitBlockTest() throws Exception {
    TestHarness harness = new TestHarness();
    long blockId = harness.mRandom.nextLong();
    long length = harness.mRandom.nextLong();
    long lockId = harness.mRandom.nextLong();
    long sessionId = harness.mRandom.nextLong();
    long usedBytes = harness.mRandom.nextLong();
    int tierAlias = 1;
    LinkedList<Long> usedBytesOnTiers = new LinkedList<Long>();
    usedBytesOnTiers.add(usedBytes);
    BlockMeta blockMeta = Mockito.mock(BlockMeta.class);
    BlockStoreLocation blockStoreLocation = Mockito.mock(BlockStoreLocation.class);
    BlockStoreMeta blockStoreMeta = Mockito.mock(BlockStoreMeta.class);

    Mockito.when(harness.mBlockStore.lockBlock(sessionId, blockId)).thenReturn(lockId);
    Mockito.when(harness.mBlockStore.getBlockMeta(sessionId, blockId, lockId))
        .thenReturn(blockMeta);
    Mockito.when(harness.mBlockStore.getBlockStoreMeta()).thenReturn(blockStoreMeta);
    Mockito.when(blockMeta.getBlockLocation()).thenReturn(blockStoreLocation);
    Mockito.when(blockStoreLocation.tierAlias()).thenReturn(tierAlias);
    Mockito.when(blockMeta.getBlockSize()).thenReturn(length);
    Mockito.when(blockStoreMeta.getUsedBytesOnTiers()).thenReturn(usedBytesOnTiers);

    harness.mManager.commitBlock(sessionId, blockId);
    Mockito.verify(harness.mBlockMasterClient)
        .workerCommitBlock(harness.mWorkerId, usedBytes, tierAlias, blockId, length);
    Mockito.verify(harness.mBlockStore).unlockBlock(lockId);
  }

  @Test
  public void CreateBlockTest() throws Exception {
    TestHarness harness = new TestHarness();
    long blockId = harness.mRandom.nextLong();
    long initialBytes = harness.mRandom.nextLong();
    long sessionId = harness.mRandom.nextLong();
    int tierAlias = 1;
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    StorageDir storageDir = Mockito.mock(StorageDir.class);
    TempBlockMeta meta = new TempBlockMeta(sessionId, blockId, initialBytes, storageDir);

    Mockito.when(harness.mBlockStore
        .createBlockMeta(sessionId, blockId, location, initialBytes)).thenReturn(meta);
    Mockito.when(storageDir.getDirPath()).thenReturn("/tmp");
    Assert.assertEquals(PathUtils.concatPath("/tmp", sessionId, blockId),
        harness.mManager.createBlock(sessionId, blockId, tierAlias, initialBytes));
  }

  // TODO(jsimsa): Write unit tests for untested public methods.
}