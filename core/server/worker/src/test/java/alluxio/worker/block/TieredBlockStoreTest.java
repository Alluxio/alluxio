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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.test.util.ConcurrencyUtils;
import alluxio.util.CommonUtils;
import alluxio.util.io.FileUtils;
import alluxio.worker.block.annotator.BlockIterator;
import alluxio.worker.block.evictor.EvictionPlan;
import alluxio.worker.block.evictor.Evictor;
import alluxio.worker.block.evictor.Evictor.Mode;
import alluxio.worker.block.meta.DefaultBlockMeta;
import alluxio.worker.block.meta.DefaultTempBlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link TieredBlockStore}.
 */
public final class TieredBlockStoreTest {
  private static final long SESSION_ID1 = 2;
  private static final long SESSION_ID2 = 3;
  private static final long BLOCK_ID1 = 1000;
  private static final long BLOCK_ID2 = 1001;
  private static final long TEMP_BLOCK_ID = 1003;
  private static final long TEMP_BLOCK_ID2 = 1004;
  private static final long BLOCK_SIZE = 512;
  private static final String FIRST_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[0];
  private static final String SECOND_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[1];
  private TieredBlockStore mBlockStore;
  private BlockMetadataManager mMetaManager;
  private BlockLockManager mLockManager;
  private StorageDir mTestDir1;
  private StorageDir mTestDir2;
  private StorageDir mTestDir3;
  private StorageDir mTestDir4;
  private BlockIterator mBlockIterator;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
    init(0);
  }

  private void init(long reservedBytes) throws Exception {
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_CLASS,
            "alluxio.worker.block.reviewer.AcceptingReviewer");
    // No reserved space for tests that are not swap related.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES, reservedBytes);

    File tempFolder = mTestFolder.newFolder();
    TieredBlockStoreTestUtils.setupDefaultConf(tempFolder.getAbsolutePath());
    mMetaManager = BlockMetadataManager.createBlockMetadataManager();
    mLockManager = new BlockLockManager();
    mBlockStore = new TieredBlockStore(mMetaManager, mLockManager);
    mBlockIterator = mMetaManager.getBlockIterator();

    mTestDir1 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(0);
    mTestDir2 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(1);
    mTestDir3 = mMetaManager.getTier(SECOND_TIER_ALIAS).getDir(1);
    mTestDir4 = mMetaManager.getTier(SECOND_TIER_ALIAS).getDir(2);
  }

  /**
   * Tests that different sessions can concurrently grab block locks on different blocks.
   */
  @Test
  public void differentSessionLockDifferentBlocks() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    TieredBlockStoreTestUtils.cache2(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir2, mMetaManager,
        mBlockIterator);

    long lockId1 = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID1);
    assertTrue(
        Sets.difference(mLockManager.getLockedBlocks(), Sets.newHashSet(BLOCK_ID1)).isEmpty());

    long lockId2 = mBlockStore.lockBlock(SESSION_ID2, BLOCK_ID2);
    assertNotEquals(lockId1, lockId2);
    assertTrue(
        Sets.difference(mLockManager.getLockedBlocks(), Sets.newHashSet(BLOCK_ID1, BLOCK_ID2))
            .isEmpty());

    mBlockStore.unlockBlock(lockId2);
    assertTrue(
        Sets.difference(mLockManager.getLockedBlocks(), Sets.newHashSet(BLOCK_ID1)).isEmpty());

    mBlockStore.unlockBlock(lockId1);
    assertTrue(mLockManager.getLockedBlocks().isEmpty());
  }

  /**
   * Same session can concurrently grab block locks on different block.
   */
  @Test
  public void sameSessionLockDifferentBlocks() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID2, BLOCK_SIZE, mTestDir2, mMetaManager,
        mBlockIterator);

    long lockId1 = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID1);
    assertTrue(
        Sets.difference(mLockManager.getLockedBlocks(), Sets.newHashSet(BLOCK_ID1)).isEmpty());

    long lockId2 = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID2);
    assertNotEquals(lockId1, lockId2);
  }

  /**
   * Tests that an exception is thrown when trying to lock a block which doesn't exist.
   */
  @Test
  public void lockNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.NO_BLOCK_ID_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID1);
  }

  /**
   * Tests that an exception is thrown when trying to unlock a block which doesn't exist.
   */
  @Test
  public void unlockNonExistingLock() throws Exception {
    long badLockId = 1003;
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(badLockId));

    mBlockStore.unlockBlock(badLockId);
  }

  /**
   * Tests the {@link TieredBlockStore#commitBlock(long, long, boolean)} method.
   */
  @Test
  public void commitBlock() throws Exception {
    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    assertFalse(mBlockStore.hasBlockMeta(TEMP_BLOCK_ID));
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID, false);
    assertTrue(mBlockStore.hasBlockMeta(TEMP_BLOCK_ID));
    assertFalse(
        FileUtils.exists(DefaultTempBlockMeta.tempPath(mTestDir1, SESSION_ID1, TEMP_BLOCK_ID)));
    assertTrue(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, TEMP_BLOCK_ID)));
  }

  /**
   * Tests the {@link TieredBlockStore#abortBlock(long, long)} method.
   */
  @Test
  public void abortBlock() throws Exception {
    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.abortBlock(SESSION_ID1, TEMP_BLOCK_ID);
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(mBlockStore.hasBlockMeta(TEMP_BLOCK_ID));
    assertFalse(
        FileUtils.exists(DefaultTempBlockMeta.tempPath(mTestDir1, SESSION_ID1, TEMP_BLOCK_ID)));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, TEMP_BLOCK_ID)));
  }

  /**
   * Tests the {@link TieredBlockStore#moveBlock(long, long, AllocateOptions)} method.
   */
  @Test
  public void moveBlock() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1,
        AllocateOptions.forMove(mTestDir2.toBlockStoreLocation()));
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertTrue(mTestDir2.hasBlockMeta(BLOCK_ID1));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
    assertTrue(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir2, BLOCK_ID1)));

    // Move block from the specific Dir
    TieredBlockStoreTestUtils.cache2(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    // Move block from wrong Dir
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION.getMessage(BLOCK_ID2,
        mTestDir2.toBlockStoreLocation()));
    mBlockStore.moveBlock(SESSION_ID2, BLOCK_ID2, mTestDir2.toBlockStoreLocation(),
        AllocateOptions.forMove(mTestDir3.toBlockStoreLocation()));
    // Move block from right Dir
    mBlockStore.moveBlock(SESSION_ID2, BLOCK_ID2, mTestDir1.toBlockStoreLocation(),
        AllocateOptions.forMove(mTestDir3.toBlockStoreLocation()));
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertTrue(mTestDir3.hasBlockMeta(BLOCK_ID2));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID2)));
    assertTrue(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir3, BLOCK_ID2)));

    // Move block from the specific tier
    mBlockStore.moveBlock(SESSION_ID2, BLOCK_ID2,
        BlockStoreLocation.anyDirInTier(mTestDir1.getParentTier().getTierAlias()),
        AllocateOptions.forMove(mTestDir3.toBlockStoreLocation()));
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertTrue(mTestDir3.hasBlockMeta(BLOCK_ID2));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID2)));
    assertTrue(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir3, BLOCK_ID2)));
  }

  @Test
  public void tierMoveTargetIsFull() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
            mBlockIterator);
    // Fill up the target dir
    TieredBlockStoreTestUtils.cache2(SESSION_ID2, BLOCK_ID2, mTestDir2.getCapacityBytes(),
            mTestDir2, mMetaManager, mBlockIterator);
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE.getMessage(
            mTestDir2.toBlockStoreLocation(), BLOCK_ID1));
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1,
            AllocateOptions.forTierMove(mTestDir2.toBlockStoreLocation()));
    // Consistency check: the block is still in its original location
    assertTrue(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(mTestDir2.hasBlockMeta(BLOCK_ID1));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID1));
    assertTrue(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir2, BLOCK_ID1)));
  }

  /**
   * Tests that moving a block to the same location does nothing.
   */
  @Test
  public void moveBlockToSameLocation() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    // Move block to same location will simply do nothing, so the src block keeps where it was,
    // and the available space should also remain unchanged.
    long availableBytesBefore = mMetaManager.getAvailableBytes(mTestDir1.toBlockStoreLocation());
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1,
        AllocateOptions.forMove(mTestDir1.toBlockStoreLocation()));
    long availableBytesAfter = mMetaManager.getAvailableBytes(mTestDir1.toBlockStoreLocation());

    assertEquals(availableBytesBefore, availableBytesAfter);
    assertTrue(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(mMetaManager.hasTempBlockMeta(BLOCK_ID1));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID1));
    assertTrue(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
  }

  /**
   * Tests the {@link TieredBlockStore#removeBlock(long, long)} method.
   */
  @Test
  public void removeBlock() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    mBlockStore.removeBlock(SESSION_ID1, BLOCK_ID1);
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(mBlockStore.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID1)));

    // Remove block from specific Dir
    TieredBlockStoreTestUtils.cache2(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    // Remove block from wrong Dir
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION.getMessage(BLOCK_ID2,
        mTestDir2.toBlockStoreLocation()));
    mBlockStore.removeBlock(SESSION_ID2, BLOCK_ID2, mTestDir2.toBlockStoreLocation());
    // Remove block from right Dir
    mBlockStore.removeBlock(SESSION_ID2, BLOCK_ID2, mTestDir1.toBlockStoreLocation());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertFalse(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID2)));

    // Remove block from the specific tier
    TieredBlockStoreTestUtils.cache2(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    mBlockStore.removeBlock(SESSION_ID2, BLOCK_ID2,
        BlockStoreLocation.anyDirInTier(mTestDir1.getParentTier().getTierAlias()));
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertFalse(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID2)));
  }

  /**
   * Tests the {@link TieredBlockStore#freeSpace(long, long, long, BlockStoreLocation)} method.
   */
  @Test
  public void freeSpace() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes(), mTestDir1.getCapacityBytes(),
        mTestDir1.toBlockStoreLocation());
    // Expect BLOCK_ID1 to be moved out of mTestDir1
    assertEquals(mTestDir1.getCapacityBytes(), mTestDir1.getAvailableBytes());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
  }

  @Test
  public void freeSpaceAhead() throws Exception {
    long halfCapacityBytes = mTestDir1.getCapacityBytes() / 2;
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, halfCapacityBytes, mTestDir1,
        mMetaManager, mBlockIterator);
    TieredBlockStoreTestUtils.cache2(SESSION_ID2, BLOCK_ID2, halfCapacityBytes, mTestDir1,
        mMetaManager, mBlockIterator);
    mBlockStore.freeSpace(SESSION_ID1, halfCapacityBytes, 2 * halfCapacityBytes,
        mTestDir1.toBlockStoreLocation());
    // Expect BLOCK_ID1 and BLOCK_ID2 to be moved out of mTestDir1
    assertEquals(mTestDir1.getCapacityBytes(), mTestDir1.getAvailableBytes());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
    assertFalse(FileUtils.exists(DefaultBlockMeta.commitPath(mTestDir1, BLOCK_ID2)));
  }

  /**
   * Tests the {@link TieredBlockStore#freeSpace(long, long, long, BlockStoreLocation)} method.
   */
  @Test
  public void freeSpaceThreadSafe() throws Exception {
    int threadAmount = 10;
    List<Runnable> runnables = new ArrayList<>();
    Evictor evictor = mock(Evictor.class);
    when(
        evictor.freeSpaceWithView(any(Long.class), any(BlockStoreLocation.class),
            any(BlockMetadataEvictorView.class), any(Mode.class)))
        .thenAnswer((InvocationOnMock invocation) -> {
          CommonUtils.sleepMs(20);
          return new EvictionPlan(new ArrayList<>(), new ArrayList<>());
        }
        );
    for (int i = 0; i < threadAmount; i++) {
      runnables.add(() -> {
        try {
          mBlockStore.freeSpace(SESSION_ID1, 0, 0,
              new BlockStoreLocation(Constants.MEDIUM_MEM, 0));
        } catch (Exception e) {
          fail();
        }
      });
    }
    RetryPolicy retry = new CountingRetry(threadAmount);
    while (retry.attempt()) {
      ConcurrencyUtils.assertConcurrent(runnables, threadAmount);
    }
  }

  /**
   * Tests the {@link TieredBlockStore#freeSpace(long, long, long, BlockStoreLocation)} method.
   */
  @Test
  public void freeSpaceWithPinnedBlocks() throws Exception {
    // create a pinned block
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mBlockStore,
        mTestDir1.toBlockStoreLocation(), true);

    // Expect an empty eviction plan is feasible
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE.getMessage(
        mTestDir1.getCapacityBytes(), mTestDir1.toBlockStoreLocation().tierAlias()));
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes(), mTestDir1.getCapacityBytes(),
        mTestDir1.toBlockStoreLocation());
  }

  /**
   * Tests the {@link TieredBlockStore#requestSpace(long, long, long)} method.
   */
  @Test
  public void requestSpace() throws Exception {
    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, 1, mTestDir1);
    mBlockStore.requestSpace(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE - 1);
    assertTrue(mTestDir1.hasTempBlockMeta(TEMP_BLOCK_ID));
    assertEquals(BLOCK_SIZE, mTestDir1.getTempBlockMeta(TEMP_BLOCK_ID).getBlockSize());
    assertEquals(mTestDir1.getCapacityBytes() - BLOCK_SIZE, mTestDir1.getAvailableBytes());
  }

  /**
   * Tests the {@link TieredBlockStore#createBlock(long, long, AllocateOptions)} method
   * to work without eviction.
   */
  @Test
  public void createBlockMetaWithoutEviction() throws Exception {
    TempBlockMeta tempBlockMeta = mBlockStore.createBlock(SESSION_ID1, TEMP_BLOCK_ID,
        AllocateOptions.forCreate(1, mTestDir1.toBlockStoreLocation()));
    assertEquals(1, tempBlockMeta.getBlockSize());
    assertEquals(mTestDir1, tempBlockMeta.getParentDir());
  }

  @Test
  public void createBlockMetaWithMediumType() throws Exception {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInAnyTierWithMedium(Constants.MEDIUM_MEM);
    TempBlockMeta tempBlockMeta = mBlockStore.createBlock(SESSION_ID1, TEMP_BLOCK_ID,
        AllocateOptions.forCreate(1, loc));
    assertEquals(1, tempBlockMeta.getBlockSize());
    assertEquals(mTestDir2, tempBlockMeta.getParentDir());

    BlockStoreLocation loc2 = BlockStoreLocation.anyDirInAnyTierWithMedium(Constants.MEDIUM_SSD);
    TempBlockMeta tempBlockMeta2 = mBlockStore.createBlock(SESSION_ID1, TEMP_BLOCK_ID2,
        AllocateOptions.forCreate(1, loc2));
    assertEquals(1, tempBlockMeta2.getBlockSize());
    assertEquals(mTestDir4, tempBlockMeta2.getParentDir());
  }

  /**
   * Tests that when creating a block, if the space of the target location is currently taken by
   * another block being locked, this creation will happen on the next available dir.
   */
  @Test
  public void createBlockMetaWithBlockLocked() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);

    // session1 locks a block first
    long lockId = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID1);

    // Create file that in dir1 that won't fit.
    TieredBlockStoreTestUtils.cache(SESSION_ID2, TEMP_BLOCK_ID, mTestDir1.getCapacityBytes(),
        mBlockStore, mTestDir1.toBlockStoreLocation(), false);

    // unlock the original block.
    mBlockStore.unlockBlock(lockId);

    assertEquals(mTestDir1.getCapacityBytes(), mTestDir2.getCommittedBytes());
  }

  @Test
  public void relaxLocationWrites() throws Exception {
    long totalCapacityBytes = 0;
    for (StorageDir dir : new StorageDir[] {mTestDir1, mTestDir2, mTestDir3, mTestDir4}) {
      totalCapacityBytes += dir.getCapacityBytes();
    }

    long fileSizeBytes = 1000;
    int maxFileCount = (int) (totalCapacityBytes / fileSizeBytes);

    // Write to first dir, 2 times of the whole store's peak capacity.
    for (int i = 0; i <= maxFileCount * 2; i++) {
      TieredBlockStoreTestUtils.cache(i, i, fileSizeBytes, mBlockStore,
          mTestDir1.toBlockStoreLocation(), false);
    }
  }

  @Test
  public void stickyLocationWrites() throws Exception {
    long fileSizeBytes = 100;
    int maxFileCount = (int) (mTestDir1.getCapacityBytes() / fileSizeBytes);
    AllocateOptions remainOnLocOptions = AllocateOptions
        .forCreate(fileSizeBytes, mTestDir1.toBlockStoreLocation()).setForceLocation(true);

    // Write to first dir, 2 times of its peak capacity.
    for (int i = 0; i <= maxFileCount * 2; i++) {
      TieredBlockStoreTestUtils.cache(i, i, remainOnLocOptions, mBlockStore, false);
    }

    assertEquals(0, mTestDir1.getAvailableBytes());
    assertEquals(0, mTestDir2.getCommittedBytes());
    assertEquals(0, mTestDir3.getCommittedBytes());
    assertEquals(0, mTestDir4.getCommittedBytes());
  }

  /**
   * Tests that when moving a block from src location to dst, if the space of the dst location is
   * currently taken by another block being locked, this move operation will fail until the lock
   * released.
   */
  @Test
  public void moveBlockMetaWithBlockLocked() throws Exception {
    // Setup the src dir containing the block to move
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    // Setup the dst dir whose space is totally taken by another block
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID2, mTestDir2.getCapacityBytes(),
        mTestDir2, mMetaManager, mBlockIterator);

    // session1 locks block2 first
    long lockId = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID2);

    // Expect an exception because no eviction plan is feasible
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE
        .getMessage(mTestDir2.toBlockStoreLocation(), BLOCK_ID1));

    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1,
        AllocateOptions.forMove(mTestDir2.toBlockStoreLocation()));

    // Expect createBlockMeta to succeed after unlocking this block.
    mBlockStore.unlockBlock(lockId);
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1,
        AllocateOptions.forMove(mTestDir2.toBlockStoreLocation()));

    assertEquals(mTestDir1.getCapacityBytes(), mTestDir1.getAvailableBytes());
    assertEquals(mTestDir2.getCapacityBytes() - BLOCK_SIZE, mTestDir2.getAvailableBytes());
  }

  @Test
  public void moveBlockWithReservedSpace() throws Exception {
    ServerConfiguration.reset();
    final long RESERVE_SIZE = BLOCK_SIZE;
    init(RESERVE_SIZE);

    // Setup the src dir containing the block to move
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    // Setup the dst dir whose space is totally taken by another block
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID2,
        mTestDir2.getCapacityBytes() - BLOCK_SIZE, mTestDir2, mMetaManager, mBlockIterator);
    assertEquals(0, mTestDir2.getAvailableBytes());
    assertTrue(mTestDir2.getCapacityBytes() > mTestDir2.getCommittedBytes());

    // Expect an exception because destination is full.
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE
        .getMessage(mTestDir2.toBlockStoreLocation(), BLOCK_ID1));

    AllocateOptions moveOptions = AllocateOptions.forMove(mTestDir2.toBlockStoreLocation())
        .setForceLocation(true)     // Stay in destination.
        .setEvictionAllowed(false); // No eviction.

    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1, moveOptions);

    // Expect createBlockMeta to succeed after setting 'useReservedSpace' flag for allocation.
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1, moveOptions.setUseReservedSpace(true));

    // Validate source is empty.
    assertEquals(mTestDir1.getCapacityBytes(),
        mTestDir1.getAvailableBytes() + mTestDir1.getReservedBytes());
    // Validate destination is full.
    assertEquals(mTestDir2.getCapacityBytes(), mTestDir2.getCommittedBytes());
  }

  /**
   * Tests that when free the space of a location, if the space of the target location is currently
   * taken by another block being locked, this freeSpace operation will fail until the lock
   * released.
   */
  @Test
  public void freeSpaceWithBlockLocked() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);

    // session1 locks a block first
    long lockId = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID1);

    // Expect an empty eviction plan is feasible
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE.getMessage(
        mTestDir1.getCapacityBytes(), mTestDir1.toBlockStoreLocation().tierAlias()));
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes(), mTestDir1.getCapacityBytes(),
        mTestDir1.toBlockStoreLocation());

    // Expect freeSpace to succeed after unlock this block.
    mBlockStore.unlockBlock(lockId);
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes(), mTestDir1.getCapacityBytes(),
        mTestDir1.toBlockStoreLocation());
    assertEquals(mTestDir1.getCapacityBytes(), mTestDir1.getAvailableBytes());
  }

  /**
   * Tests that an exception is thrown when trying to get a writer for the block that does not
   * exist.
   */
  @Test
  public void getBlockWriterForNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.getBlockWriter(SESSION_ID1, BLOCK_ID1);
  }

  /**
   * Tests that an exception is thrown when trying to abort a block that does not exist.
   */
  @Test
  public void abortNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.abortBlock(SESSION_ID1, BLOCK_ID1);
  }

  /**
   * Tests that an exception is thrown when trying to abort a block that is not owned by the
   * session.
   */
  @Test
  public void abortBlockNotOwnedBySessionId() throws Exception {
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_ID_FOR_DIFFERENT_SESSION.getMessage(TEMP_BLOCK_ID,
        SESSION_ID1, SESSION_ID2));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.abortBlock(SESSION_ID2, TEMP_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown when trying to abort a block which was committed.
   */
  @Test
  public void abortCommitedBlock() throws Exception {
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED.getMessage(TEMP_BLOCK_ID));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID, false);
    mBlockStore.abortBlock(SESSION_ID1, TEMP_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown when trying to move a block which does not exist.
   */
  @Test
  public void moveNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1,
        AllocateOptions.forMove(mTestDir1.toBlockStoreLocation()));
  }

  /**
   * Tests that an exception is thrown when trying to move a temporary block.
   */
  @Test
  public void moveTempBlock() throws Exception {
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK.getMessage(TEMP_BLOCK_ID));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.moveBlock(SESSION_ID1, TEMP_BLOCK_ID,
        AllocateOptions.forMove(mTestDir2.toBlockStoreLocation()));
  }

  /**
   * Tests that an exception is thrown when trying to cache a block which already exists in a
   * different directory.
   */
  @Test
  public void cacheSameBlockInDifferentDirs() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(BLOCK_ID1,
        FIRST_TIER_ALIAS));
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir2, mMetaManager,
        mBlockIterator);
  }

  /**
   * Tests that an exception is thrown when trying to cache a block which already exists in a
   * different tier.
   */
  @Test
  public void cacheSameBlockInDifferentTiers() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(BLOCK_ID1,
        FIRST_TIER_ALIAS));
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir3, mMetaManager,
        mBlockIterator);
  }

  /**
   * Tests that an exception is thrown when trying to commit a block twice.
   */
  @Test
  public void commitBlockTwice() throws Exception {
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED.getMessage(TEMP_BLOCK_ID));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID , false);
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID, false);
  }

  /**
   * Tests that an exception is thrown when trying to commit a block which does not exist.
   */
  @Test
  public void commitNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.commitBlock(SESSION_ID1, BLOCK_ID1, false);
  }

  /**
   * Tests that an exception is thrown when trying to commit a block which is not owned by the
   * session.
   */
  @Test
  public void commitBlockNotOwnedBySessionId() throws Exception {
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_ID_FOR_DIFFERENT_SESSION.getMessage(TEMP_BLOCK_ID,
        SESSION_ID1, SESSION_ID2));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.commitBlock(SESSION_ID2, TEMP_BLOCK_ID, false);
  }

  /**
   * Tests that an exception is thrown when trying to remove a block which was not committed.
   */
  @Test
  public void removeTempBlock() throws Exception {
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.REMOVE_UNCOMMITTED_BLOCK.getMessage(TEMP_BLOCK_ID));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.removeBlock(SESSION_ID1, TEMP_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown when trying to remove a block which does not exist.
   */
  @Test
  public void removeNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.removeBlock(SESSION_ID1, BLOCK_ID1);
  }

  /**
   * Tests that check storage fails when a directory is inaccessible.
   */
  @Test
  public void checkStorageFailed() throws Exception {
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mBlockIterator);
    TieredBlockStoreTestUtils.cache2(SESSION_ID1, BLOCK_ID2, BLOCK_SIZE, mTestDir2, mMetaManager,
        mBlockIterator);
    BlockStoreMeta oldMeta = mBlockStore.getBlockStoreMeta();
    FileUtils.deletePathRecursively(mTestDir2.getDirPath());
    mBlockStore.removeInaccessibleStorage();
    BlockStoreMeta meta = mBlockStore.getBlockStoreMetaFull();
    long usedByteInDir = mTestDir2.getCapacityBytes() - mTestDir2.getAvailableBytes();
    assertFalse("failed storage path should be removed",
        meta.getDirectoryPathsOnTiers().get(FIRST_TIER_ALIAS).contains(mTestDir2.getDirPath()));
    assertEquals("failed storage path quota should be deducted from store capacity",
        oldMeta.getCapacityBytes() - mTestDir2.getCapacityBytes(), meta.getCapacityBytes());
    assertEquals("failed storage path used bytes should be deducted from store used bytes",
        oldMeta.getUsedBytes() - usedByteInDir,
        meta.getUsedBytes());
    assertEquals("failed storage path quota should be deducted from tier capacity",
        oldMeta.getCapacityBytesOnTiers().get(FIRST_TIER_ALIAS) - mTestDir2.getCapacityBytes(),
        (long) meta.getCapacityBytesOnTiers().get(FIRST_TIER_ALIAS));
    assertEquals("failed storage path used bytes should be deducted from tier used bytes",
        oldMeta.getUsedBytesOnTiers().get(FIRST_TIER_ALIAS) - usedByteInDir,
        (long) meta.getUsedBytesOnTiers().get(FIRST_TIER_ALIAS));
    assertFalse("blocks in failed storage path should be removed",
        meta.getBlockList().get(FIRST_TIER_ALIAS).contains(BLOCK_ID2));
    assertTrue("blocks in working storage path should be retained",
        meta.getBlockList().get(FIRST_TIER_ALIAS).contains(BLOCK_ID1));
  }
}
