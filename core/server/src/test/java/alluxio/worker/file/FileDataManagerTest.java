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

package alluxio.worker.file;

import static org.junit.Assert.assertEquals;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MockRateLimiter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Set;

/**
 * Tests {@link FileDataManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class, BufferUtils.class, BlockMeta.class})
public final class FileDataManagerTest {

  /**
   * Resets the worker context.
   */
  @After
  public void after() throws IOException {
    WorkerContext.reset();
  }

  /**
   * Tests that a file gets persisted.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  @SuppressWarnings("unchecked")
  public void persistFileTest() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L);

    // mock block worker
    BlockWorker blockWorker = Mockito.mock(BlockWorker.class);
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    Mockito.when(blockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    BlockReader reader = Mockito.mock(BlockReader.class);
    for (long blockId : blockIds) {
      Mockito.when(blockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito
          .when(blockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(reader);
    }

    FileDataManager manager = new FileDataManager(blockWorker);

    // mock ufs
    UnderFileSystem ufs = Mockito.mock(UnderFileSystem.class);
    String ufsRoot = new Configuration().get(Constants.UNDERFS_ADDRESS);
    Mockito.when(ufs.exists(ufsRoot)).thenReturn(true);
    Whitebox.setInternalState(manager, "mUfs", ufs);
    OutputStream outputStream = Mockito.mock(OutputStream.class);

    // mock BufferUtils
    PowerMockito.mockStatic(BufferUtils.class);

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    Mockito.when(ufs.create(dstPath)).thenReturn(outputStream);

    manager.lockBlocks(fileId, blockIds);
    manager.persistFile(fileId, blockIds);

    // verify file persisted
    Set<Long> persistedFiles = (Set<Long>) Whitebox.getInternalState(manager, "mPersistedFiles");
    Assert.assertEquals(Sets.newHashSet(fileId), persistedFiles);

    // verify fastCopy called twice, once per block
    PowerMockito.verifyStatic(Mockito.times(2));
    BufferUtils.fastCopy(Mockito.any(ReadableByteChannel.class),
        Mockito.any(WritableByteChannel.class));

    // verify the file is not needed for another persistence
    Assert.assertFalse(manager.needPersistence(fileId));
  }

  /**
   * Tests that persisted file are cleared in the manager.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void clearPersistedFilesTest() {
    BlockWorker blockWorker = Mockito.mock(BlockWorker.class);
    FileDataManager manager = new FileDataManager(blockWorker);
    Set<Long> persistedFiles = Sets.newHashSet(1L, 2L);

    Whitebox.setInternalState(manager, "mPersistedFiles", Sets.newHashSet(persistedFiles));
    List<Long> poppedList = manager.getPersistedFiles();
    Assert.assertEquals(persistedFiles, Sets.newHashSet(poppedList));

    // verify persisted files cleared in the manager
    poppedList.remove(2L);
    manager.clearPersistedFiles(poppedList);
    persistedFiles = (Set<Long>) Whitebox.getInternalState(manager, "mPersistedFiles");
    Assert.assertEquals(Sets.newHashSet(2L), persistedFiles);
  }

  /**
   * Tests the rate limiting functionality for asynchronous persistence.
   */
  @Test
  public void persistFileRateLimitingTest() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L, 3L);

    // mock block worker
    BlockWorker blockWorker = Mockito.mock(BlockWorker.class);
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    Mockito.when(blockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    BlockReader reader = Mockito.mock(BlockReader.class);
    for (long blockId : blockIds) {
      Mockito.when(blockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito
          .when(blockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(reader);
      BlockMeta mockedBlockMeta = PowerMockito.mock(BlockMeta.class);
      Mockito.when(mockedBlockMeta.getBlockSize()).thenReturn(100L);
      Mockito
          .when(blockWorker.getBlockMeta(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(mockedBlockMeta);
    }

    Configuration conf = WorkerContext.getConf();
    conf.set(Constants.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED, "true");
    conf.set(Constants.WORKER_FILE_PERSIST_RATE_LIMIT, "100");

    FileDataManager manager = new FileDataManager(blockWorker);

    // mock ufs
    UnderFileSystem ufs = Mockito.mock(UnderFileSystem.class);
    String ufsRoot = new Configuration().get(Constants.UNDERFS_ADDRESS);
    Mockito.when(ufs.exists(ufsRoot)).thenReturn(true);
    Whitebox.setInternalState(manager, "mUfs", ufs);

    // Setup a mock rate limiter.
    MockRateLimiter mockRateLimiter = new MockRateLimiter(
        WorkerContext.getConf().getBytes(Constants.WORKER_FILE_PERSIST_RATE_LIMIT));
    Whitebox.setInternalState(
        manager, "mPersistenceRateLimiter", mockRateLimiter.getGuavaRateLimiter());

    OutputStream outputStream = Mockito.mock(OutputStream.class);

    // mock BufferUtils
    PowerMockito.mockStatic(BufferUtils.class);

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    Mockito.when(ufs.create(dstPath)).thenReturn(outputStream);

    manager.lockBlocks(fileId, blockIds);
    manager.persistFile(fileId, blockIds);

    List<String> expectedEvents = Lists.newArrayList("R0.00", "R1.00", "R1.00");
    assertEquals(expectedEvents, mockRateLimiter.readEventsAndClear());

    // Simulate waiting for 1 second.
    mockRateLimiter.sleepMillis(1000);

    manager.lockBlocks(fileId, blockIds);
    manager.persistFile(fileId, blockIds);

    // The first write will go through immediately without throttling.
    expectedEvents = Lists.newArrayList("U1.00", "R0.00", "R1.00", "R1.00");
    assertEquals(expectedEvents, mockRateLimiter.readEventsAndClear());

    // Repeat persistence without sleeping.
    mockRateLimiter = new MockRateLimiter(
        WorkerContext.getConf().getBytes(Constants.WORKER_FILE_PERSIST_RATE_LIMIT));
    Whitebox.setInternalState(
        manager, "mPersistenceRateLimiter", mockRateLimiter.getGuavaRateLimiter());

    manager.lockBlocks(fileId, blockIds);
    manager.persistFile(fileId, blockIds);

    expectedEvents = Lists.newArrayList("R0.00", "R1.00", "R1.00");
    assertEquals(expectedEvents, mockRateLimiter.readEventsAndClear());

    manager.lockBlocks(fileId, blockIds);
    manager.persistFile(fileId, blockIds);

    expectedEvents = Lists.newArrayList("R1.00", "R1.00", "R1.00");
    assertEquals(expectedEvents, mockRateLimiter.readEventsAndClear());
  }

  /**
   * Tests the blocks are unlocked correctly when exception is encountered in
   * {@link FileDataManager#lockBlocks(long, List)}.
   *
   * @throws Exception when an exception occurs
   */
  @Test
  public void lockBlocksErrorHandlingTest() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L, 3L);

    // mock block data manager
    BlockWorker blockWorker = Mockito.mock(BlockWorker.class);
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    Mockito.when(blockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 1L)).thenReturn(1L);
    Mockito.when(blockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 2L)).thenReturn(2L);
    Mockito.when(blockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 3L))
        .thenThrow(new BlockDoesNotExistException("block 3 does not exist"));
    FileDataManager manager = new FileDataManager(blockWorker);
    try {
      manager.lockBlocks(fileId, blockIds);
      Assert.fail("the lock should fail");
    } catch (IOException e) {
      Assert.assertEquals(
          "failed to lock all blocks of file 1\n"
              + "alluxio.exception.BlockDoesNotExistException: block 3 does not exist\n",
          e.getMessage());
      // verify the locks are all unlocked
      Mockito.verify(blockWorker).unlockBlock(1L);
      Mockito.verify(blockWorker).unlockBlock(2L);
    }
  }

  /**
   * Tests that the correct error message is provided when persisting a file fails.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void errorHandlingTest() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L);

    // mock block worker
    BlockWorker blockWorker = Mockito.mock(BlockWorker.class);
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    Mockito.when(blockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    for (long blockId : blockIds) {
      Mockito.when(blockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito.doThrow(new InvalidWorkerStateException("invalid worker")).when(blockWorker)
          .readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId);
    }

    FileDataManager manager = new FileDataManager(blockWorker);

    // mock ufs
    UnderFileSystem ufs = Mockito.mock(UnderFileSystem.class);
    String ufsRoot = new Configuration().get(Constants.UNDERFS_ADDRESS);
    Mockito.when(ufs.exists(ufsRoot)).thenReturn(true);
    Whitebox.setInternalState(manager, "mUfs", ufs);
    OutputStream outputStream = Mockito.mock(OutputStream.class);

    // mock BufferUtils
    PowerMockito.mockStatic(BufferUtils.class);
    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    Mockito.when(ufs.create(dstPath)).thenReturn(outputStream);

    manager.lockBlocks(fileId, blockIds);
    try {
      manager.persistFile(fileId, blockIds);
      Assert.fail("the persist should fail");
    } catch (IOException e) {
      Assert.assertEquals("the blocks of file1 are failed to persist\n"
          + "alluxio.exception.InvalidWorkerStateException: invalid worker\n", e.getMessage());
      // verify the locks are all unlocked
      Mockito.verify(blockWorker).unlockBlock(1L);
      Mockito.verify(blockWorker).unlockBlock(2L);
    }
  }
}
