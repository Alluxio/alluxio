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
import alluxio.underfs.UfsManager.UfsInfo;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.io.BufferUtils;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link FileDataManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockWorker.class, BufferUtils.class, BlockMeta.class, FileSystem.class})
public final class FileDataManagerTest {
  private UnderFileSystem mUfs;
  private UfsManager mUfsManager;
  private BlockWorker mBlockWorker;
  private MockRateLimiter mMockRateLimiter;
  private FileDataManager mManager;
  private FileSystem mMockFileSystem;

  @Before
  public void before() throws Exception {
    mUfs = Mockito.mock(UnderFileSystem.class);
    mUfsManager = Mockito.mock(UfsManager.class);
    mBlockWorker = Mockito.mock(BlockWorker.class);
    mMockRateLimiter =
        new MockRateLimiter(Configuration.getBytes(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT));
    mManager =
        new FileDataManager(mBlockWorker, mMockRateLimiter.getGuavaRateLimiter(), mUfsManager);

    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    UfsInfo ufsInfo = new UfsInfo(Suppliers.ofInstance(mUfs), AlluxioURI.EMPTY_URI);
    PowerMockito.mockStatic(FileSystem.Factory.class);
    Mockito.when(FileSystem.Factory.get()).thenReturn(mMockFileSystem);
    Mockito.when(mUfs.isDirectory(Mockito.anyString())).thenReturn(true);
    Mockito.when(mUfsManager.get(Mockito.anyLong())).thenReturn(ufsInfo);
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
    assertEquals(Arrays.asList(fileId), mManager.getPersistedFiles());

    // verify fastCopy called twice, once per block
    PowerMockito.verifyStatic(Mockito.times(2));
    BufferUtils.fastCopy(Mockito.any(ReadableByteChannel.class),
        Mockito.any(WritableByteChannel.class));

    // verify the file is not needed for another persistence
    Assert.assertFalse(mManager.needPersistence(fileId));
  }

  /**
   * Tests that persisted file are cleared in the manager.
   */
  @Test
  public void clearPersistedFiles() throws Exception {
    writeFileWithBlocks(1L, ImmutableList.of(2L, 3L));
    mManager.clearPersistedFiles(ImmutableList.of(1L));
    assertEquals(Collections.emptyList(), mManager.getPersistedFiles());
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
    mManager =
        new FileDataManager(mBlockWorker, mMockRateLimiter.getGuavaRateLimiter(), mUfsManager);

    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L, 3L);

    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    Mockito.when(mBlockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    BlockReader reader = Mockito.mock(BlockReader.class);
    for (long blockId : blockIds) {
      Mockito.when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito.when(mBlockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(reader);
      BlockMeta mockedBlockMeta = PowerMockito.mock(BlockMeta.class);
      Mockito.when(mockedBlockMeta.getBlockSize()).thenReturn(100L);
      Mockito.when(mBlockWorker.getBlockMeta(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(mockedBlockMeta);
    }

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Mockito.when(mUfs.isDirectory(ufsRoot)).thenReturn(true);

    OutputStream outputStream = Mockito.mock(OutputStream.class);

    // mock BufferUtils
    PowerMockito.mockStatic(BufferUtils.class);

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    fileInfo.setUfsPath(dstPath);
    Mockito.when(mUfs.create(dstPath)).thenReturn(outputStream);
    Mockito.when(mUfs.create(Mockito.anyString(), Mockito.any(CreateOptions.class)))
        .thenReturn(outputStream);
    Mockito.when(mMockFileSystem.getStatus(Mockito.any(AlluxioURI.class))).thenReturn(
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

    Mockito.when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 1L)).thenReturn(1L);
    Mockito.when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 2L)).thenReturn(2L);
    Mockito.when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, 3L))
        .thenThrow(new BlockDoesNotExistException("block 3 does not exist"));
    try {
      mManager.lockBlocks(fileId, blockIds);
      Assert.fail("the lock should fail");
    } catch (IOException e) {
      assertEquals(
          "failed to lock all blocks of file 1\n"
              + "alluxio.exception.BlockDoesNotExistException: block 3 does not exist\n",
          e.getMessage());
      // verify the locks are all unlocked
      Mockito.verify(mBlockWorker).unlockBlock(1L);
      Mockito.verify(mBlockWorker).unlockBlock(2L);
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
    Mockito.when(mBlockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    for (long blockId : blockIds) {
      Mockito.when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito.doThrow(new InvalidWorkerStateException("invalid worker")).when(mBlockWorker)
          .readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId);
    }

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Mockito.when(mUfs.isDirectory(ufsRoot)).thenReturn(true);
    OutputStream outputStream = Mockito.mock(OutputStream.class);

    // mock BufferUtils
    PowerMockito.mockStatic(BufferUtils.class);
    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    fileInfo.setUfsPath(dstPath);
    Mockito.when(mUfs.create(dstPath)).thenReturn(outputStream);
    Mockito.when(mUfs.create(Mockito.anyString(), Mockito.any(CreateOptions.class)))
        .thenReturn(outputStream);
    Mockito.when(mMockFileSystem.getStatus(Mockito.any(AlluxioURI.class))).thenReturn(
        new URIStatus(fileInfo));

    mManager.lockBlocks(fileId, blockIds);
    try {
      mManager.persistFile(fileId, blockIds);
      Assert.fail("the persist should fail");
    } catch (IOException e) {
      assertEquals("the blocks of file1 are failed to persist\n"
          + "alluxio.exception.InvalidWorkerStateException: invalid worker\n", e.getMessage());
      // verify the locks are all unlocked
      Mockito.verify(mBlockWorker).unlockBlock(1L);
      Mockito.verify(mBlockWorker).unlockBlock(2L);
    }
  }

  private void writeFileWithBlocks(long fileId, List<Long> blockIds) throws Exception {
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath("test");
    Mockito.when(mBlockWorker.getFileInfo(fileId)).thenReturn(fileInfo);
    BlockReader reader = Mockito.mock(BlockReader.class);
    for (long blockId : blockIds) {
      Mockito.when(mBlockWorker.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito.when(mBlockWorker.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(reader);
    }

    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Mockito.when(mUfs.isDirectory(ufsRoot)).thenReturn(true);
    OutputStream outputStream = Mockito.mock(OutputStream.class);

    // mock BufferUtils
    PowerMockito.mockStatic(BufferUtils.class);

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    fileInfo.setUfsPath(dstPath);
    Mockito.when(mUfs.create(dstPath)).thenReturn(outputStream);
    Mockito.when(mUfs.create(Mockito.anyString(), Mockito.any(CreateOptions.class)))
        .thenReturn(outputStream);
    Mockito.when(mMockFileSystem.getStatus(Mockito.any(AlluxioURI.class))).thenReturn(
        new URIStatus(fileInfo));

    mManager.lockBlocks(fileId, blockIds);
    mManager.persistFile(fileId, blockIds);
  }
}
