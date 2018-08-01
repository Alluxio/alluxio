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

package alluxio.worker.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsClient;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MockRateLimiter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link FileDataManager}.
 */
public final class FileDataManagerTest {
  private UnderFileSystem mUfs;
  private UfsManager mUfsManager;
  private BlockWorker mBlockWorker;
  private MockRateLimiter mMockRateLimiter;
  private FileDataManager mManager;
  private FileSystem mMockFileSystem;
  private AtomicInteger mCopyCounter;

  @Before
  public void before() throws Exception {
    mUfs = mock(UnderFileSystem.class);
    mUfsManager = mock(UfsManager.class);
    mBlockWorker = mock(BlockWorker.class);
    mMockRateLimiter =
        new MockRateLimiter(Configuration.getBytes(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT));
    mCopyCounter = new AtomicInteger(0);
    mManager = new FileDataManager(mBlockWorker, mMockRateLimiter.getGuavaRateLimiter(),
        mUfsManager, () -> mMockFileSystem, (r, w) -> mCopyCounter.incrementAndGet());
    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    UfsClient ufsClient = new UfsClient(Suppliers.ofInstance(mUfs), AlluxioURI.EMPTY_URI);
    when(mUfs.isDirectory(anyString())).thenReturn(true);
    when(mUfsManager.get(anyLong())).thenReturn(ufsClient);
  }

  @After
  public void after() throws IOException {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests that a file gets persisted.
   */
  @Test
  public void persistFile() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L);

    writeFileWithBlocks(fileId, blockIds);

    // verify file persisted
    FileDataManager.PersistedFilesInfo info = mManager.getPersistedFileInfos();
    assertEquals(Arrays.asList(fileId), info.idList());

    // verify fastCopy called twice, once per block
    assertEquals(2, mCopyCounter.get());

    // verify the file is not needed for another persistence
    assertFalse(mManager.needPersistence(fileId));
  }

  /**
   * Tests that persisted file are cleared in the manager.
   */
  @Test
  public void clearPersistedFiles() throws Exception {
    writeFileWithBlocks(1L, ImmutableList.of(2L, 3L));
    mManager.clearPersistedFiles(ImmutableList.of(1L));
    FileDataManager.PersistedFilesInfo info = mManager.getPersistedFileInfos();
    assertEquals(Collections.emptyList(), info.idList());
  }

  /**
   * Tests the rate limiting functionality for asynchronous persistence.
   */
  @Test
  public void persistFileRateLimiting() throws Exception {
    Configuration.set(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT_ENABLED, "true");
    Configuration.set(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT, "100");
    mMockRateLimiter =
        new MockRateLimiter(Configuration.getBytes(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT));
    mManager = new FileDataManager(mBlockWorker, mMockRateLimiter.getGuavaRateLimiter(),
        mUfsManager, () -> mMockFileSystem, (r, w) -> mCopyCounter.incrementAndGet());

    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L, 3L);

    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    when(mBlockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    BlockReader reader = mock(BlockReader.class);
    for (long blockId : blockIds) {
      when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      when(mBlockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(reader);
      BlockMeta mockedBlockMeta = PowerMockito.mock(BlockMeta.class);
      when(mockedBlockMeta.getBlockSize()).thenReturn(100L);
      when(mBlockWorker.getBlockMeta(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(mockedBlockMeta);
    }

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    when(mUfs.isDirectory(ufsRoot)).thenReturn(true);

    OutputStream outputStream = new ByteArrayOutputStream();

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    fileInfo.setUfsPath(dstPath);
    when(mUfs.create(dstPath)).thenReturn(outputStream);
    when(mUfs.create(anyString(), any(CreateOptions.class)))
        .thenReturn(outputStream);
    when(mMockFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(
        new URIStatus(fileInfo));

    mManager.lockBlocks(fileId, blockIds);
    mManager.persistFile(fileId, blockIds);

    List<String> expectedEvents = Lists.newArrayList("R0.00", "R1.00", "R1.00");
    assertEquals(expectedEvents, mMockRateLimiter.readEventsAndClear());

    // Simulate waiting for 1 second.
    mMockRateLimiter.sleepMillis(1000);

    mManager.lockBlocks(fileId, blockIds);
    mManager.persistFile(fileId, blockIds);

    // The first write will go through immediately without throttling.
    expectedEvents = Lists.newArrayList("U1.00", "R0.00", "R1.00", "R1.00");
    assertEquals(expectedEvents, mMockRateLimiter.readEventsAndClear());

    // Repeat persistence without sleeping.
    mManager.lockBlocks(fileId, blockIds);
    mManager.persistFile(fileId, blockIds);

    expectedEvents = Lists.newArrayList("R1.00", "R1.00", "R1.00");
    assertEquals(expectedEvents, mMockRateLimiter.readEventsAndClear());
  }

  /**
   * Tests the blocks are unlocked correctly when exception is encountered in
   * {@link FileDataManager#lockBlocks(long, List)}.
   */
  @Test
  public void lockBlocksErrorHandling() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L, 3L);

    when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 1L)).thenReturn(1L);
    when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 2L)).thenReturn(2L);
    when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 3L))
        .thenThrow(new BlockDoesNotExistException("block 3 does not exist"));
    try {
      mManager.lockBlocks(fileId, blockIds);
      fail("the lock should fail");
    } catch (IOException e) {
      assertEquals(
          "failed to lock all blocks of file 1\n"
              + "alluxio.exception.BlockDoesNotExistException: block 3 does not exist\n",
          e.getMessage());
      // verify the locks are all unlocked
      verify(mBlockWorker).unlockBlock(1L);
      verify(mBlockWorker).unlockBlock(2L);
    }
  }

  /**
   * Tests that the correct error message is provided when persisting a file fails.
   */
  @Test
  public void errorHandling() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L);

    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    when(mBlockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    for (long blockId : blockIds) {
      when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      doThrow(new InvalidWorkerStateException("invalid worker")).when(mBlockWorker)
          .readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId);
    }

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    when(mUfs.isDirectory(ufsRoot)).thenReturn(true);
    OutputStream outputStream = mock(OutputStream.class);

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    fileInfo.setUfsPath(dstPath);
    when(mUfs.create(dstPath)).thenReturn(outputStream);
    when(mUfs.create(anyString(), any(CreateOptions.class)))
        .thenReturn(outputStream);
    when(mMockFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(
        new URIStatus(fileInfo));

    mManager.lockBlocks(fileId, blockIds);
    try {
      mManager.persistFile(fileId, blockIds);
      fail("the persist should fail");
    } catch (IOException e) {
      assertEquals("the blocks of file1 are failed to persist\n"
          + "alluxio.exception.InvalidWorkerStateException: invalid worker\n", e.getMessage());
      // verify the locks are all unlocked
      verify(mBlockWorker).unlockBlock(1L);
      verify(mBlockWorker).unlockBlock(2L);
    }
  }

  private void writeFileWithBlocks(long fileId, List<Long> blockIds) throws Exception {
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    when(mBlockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    BlockReader reader = mock(BlockReader.class);
    for (long blockId : blockIds) {
      when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      when(mBlockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(reader);
    }

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    when(mUfs.isDirectory(ufsRoot)).thenReturn(true);
    OutputStream outputStream = mock(OutputStream.class);

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    fileInfo.setUfsPath(dstPath);
    when(mUfs.create(dstPath)).thenReturn(outputStream);
    when(mUfs.create(anyString(), any(CreateOptions.class)))
        .thenReturn(outputStream);
    when(mMockFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(
        new URIStatus(fileInfo));

    mManager.lockBlocks(fileId, blockIds);
    mManager.persistFile(fileId, blockIds);
  }
}
