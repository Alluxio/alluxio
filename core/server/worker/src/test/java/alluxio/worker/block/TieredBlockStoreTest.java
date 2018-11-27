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

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
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
import alluxio.worker.block.evictor.EvictionPlan;
import alluxio.worker.block.evictor.Evictor;
import alluxio.worker.block.evictor.Evictor.Mode;
import alluxio.worker.block.meta.BlockMeta;
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
import java.lang.reflect.Field;
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
  private static final long BLOCK_SIZE = 512;
  private static final String FIRST_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[0];
  private static final String SECOND_TIER_ALIAS = TieredBlockStoreTestUtils.TIER_ALIAS[1];
  private TieredBlockStore mBlockStore;
  private BlockMetadataManager mMetaManager;
  private BlockLockManager mLockManager;
  private StorageDir mTestDir1;
  private StorageDir mTestDir2;
  private StorageDir mTestDir3;
  private Evictor mEvictor;

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
    ConfigurationTestUtils.resetConfiguration();
    Configuration.set(PropertyKey.WORKER_TIERED_STORE_RESERVER_ENABLED, "false");
    File tempFolder = mTestFolder.newFolder();
    TieredBlockStoreTestUtils.setupDefaultConf(tempFolder.getAbsolutePath());
    mBlockStore = new TieredBlockStore();

    // TODO(bin): Avoid using reflection to get private members.
    Field field = mBlockStore.getClass().getDeclaredField("mMetaManager");
    field.setAccessible(true);
    mMetaManager = (BlockMetadataManager) field.get(mBlockStore);
    field = mBlockStore.getClass().getDeclaredField("mLockManager");
    field.setAccessible(true);
    mLockManager = (BlockLockManager) field.get(mBlockStore);
    field = mBlockStore.getClass().getDeclaredField("mEvictor");
    field.setAccessible(true);
    mEvictor = (Evictor) field.get(mBlockStore);

    mTestDir1 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(0);
    mTestDir2 = mMetaManager.getTier(FIRST_TIER_ALIAS).getDir(1);
    mTestDir3 = mMetaManager.getTier(SECOND_TIER_ALIAS).getDir(1);
  }

  /**
   * Tests that different sessions can concurrently grab block locks on different blocks.
   */
  @Test
  public void differentSessionLockDifferentBlocks() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    TieredBlockStoreTestUtils.cache(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir2, mMetaManager,
        mEvictor);

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
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID2, BLOCK_SIZE, mTestDir2, mMetaManager,
        mEvictor);

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
   * Tests the {@link TieredBlockStore#commitBlock(long, long)} method.
   */
  @Test
  public void commitBlock() throws Exception {
    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    assertFalse(mBlockStore.hasBlockMeta(TEMP_BLOCK_ID));
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID);
    assertTrue(mBlockStore.hasBlockMeta(TEMP_BLOCK_ID));
    assertFalse(
        FileUtils.exists(TempBlockMeta.tempPath(mTestDir1, SESSION_ID1, TEMP_BLOCK_ID)));
    assertTrue(FileUtils.exists(TempBlockMeta.commitPath(mTestDir1, TEMP_BLOCK_ID)));
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
        FileUtils.exists(TempBlockMeta.tempPath(mTestDir1, SESSION_ID1, TEMP_BLOCK_ID)));
    assertFalse(FileUtils.exists(TempBlockMeta.commitPath(mTestDir1, TEMP_BLOCK_ID)));
  }

  /**
   * Tests the {@link TieredBlockStore#moveBlock(long, long, BlockStoreLocation)} method.
   */
  @Test
  public void moveBlock() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1, mTestDir2.toBlockStoreLocation());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertTrue(mTestDir2.hasBlockMeta(BLOCK_ID1));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
    assertTrue(FileUtils.exists(BlockMeta.commitPath(mTestDir2, BLOCK_ID1)));

    // Move block from the specific Dir
    TieredBlockStoreTestUtils.cache(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    // Move block from wrong Dir
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION.getMessage(BLOCK_ID2,
        mTestDir2.toBlockStoreLocation()));
    mBlockStore.moveBlock(SESSION_ID2, BLOCK_ID2, mTestDir2.toBlockStoreLocation(),
        mTestDir3.toBlockStoreLocation());
    // Move block from right Dir
    mBlockStore.moveBlock(SESSION_ID2, BLOCK_ID2, mTestDir1.toBlockStoreLocation(),
        mTestDir3.toBlockStoreLocation());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertTrue(mTestDir3.hasBlockMeta(BLOCK_ID2));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID2)));
    assertTrue(FileUtils.exists(BlockMeta.commitPath(mTestDir3, BLOCK_ID2)));

    // Move block from the specific tier
    mBlockStore.moveBlock(SESSION_ID2, BLOCK_ID2,
        BlockStoreLocation.anyDirInTier(mTestDir1.getParentTier().getTierAlias()),
        mTestDir3.toBlockStoreLocation());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertTrue(mTestDir3.hasBlockMeta(BLOCK_ID2));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID2)));
    assertTrue(FileUtils.exists(BlockMeta.commitPath(mTestDir3, BLOCK_ID2)));
  }

  /**
   * Tests that moving a block to the same location does nothing.
   */
  @Test
  public void moveBlockToSameLocation() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    // Move block to same location will simply do nothing, so the src block keeps where it was,
    // and the available space should also remain unchanged.
    long availableBytesBefore = mMetaManager.getAvailableBytes(mTestDir1.toBlockStoreLocation());
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1, mTestDir1.toBlockStoreLocation());
    long availableBytesAfter = mMetaManager.getAvailableBytes(mTestDir1.toBlockStoreLocation());

    assertEquals(availableBytesBefore, availableBytesAfter);
    assertTrue(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(mMetaManager.hasTempBlockMeta(BLOCK_ID1));
    assertTrue(mBlockStore.hasBlockMeta(BLOCK_ID1));
    assertTrue(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
  }

  /**
   * Tests the {@link TieredBlockStore#removeBlock(long, long)} method.
   */
  @Test
  public void removeBlock() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    mBlockStore.removeBlock(SESSION_ID1, BLOCK_ID1);
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(mBlockStore.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID1)));

    // Remove block from specific Dir
    TieredBlockStoreTestUtils.cache(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    // Remove block from wrong Dir
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION.getMessage(BLOCK_ID2,
        mTestDir2.toBlockStoreLocation()));
    mBlockStore.removeBlock(SESSION_ID2, BLOCK_ID2, mTestDir2.toBlockStoreLocation());
    // Remove block from right Dir
    mBlockStore.removeBlock(SESSION_ID2, BLOCK_ID2, mTestDir1.toBlockStoreLocation());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertFalse(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID2)));

    // Remove block from the specific tier
    TieredBlockStoreTestUtils.cache(SESSION_ID2, BLOCK_ID2, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    mBlockStore.removeBlock(SESSION_ID2, BLOCK_ID2,
        BlockStoreLocation.anyDirInTier(mTestDir1.getParentTier().getTierAlias()));
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID2));
    assertFalse(mBlockStore.hasBlockMeta(BLOCK_ID2));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID2)));
  }

  /**
   * Tests the {@link TieredBlockStore#freeSpace(long, long, BlockStoreLocation)} method.
   */
  @Test
  public void freeSpace() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes(),
        mTestDir1.toBlockStoreLocation());
    // Expect BLOCK_ID1 to be moved out of mTestDir1
    assertEquals(mTestDir1.getCapacityBytes(), mTestDir1.getAvailableBytes());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
  }

  /**
   * Tests the {@link TieredBlockStore#freeSpace(long, long, BlockStoreLocation)} method.
   */
  @Test
  public void freeSpaceMoreThanCapacity() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    // this call works because the space is freed in a best-effort way to move BLOCK_ID1 out
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes() * 2,
        mTestDir1.toBlockStoreLocation());
    // Expect BLOCK_ID1 to be moved out of mTestDir1
    assertEquals(mTestDir1.getCapacityBytes(), mTestDir1.getAvailableBytes());
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
  }

  /**
   * Tests the {@link TieredBlockStore#freeSpace(long, long, BlockStoreLocation)} method.
   */
  @Test
  public void freeSpaceThreadSafe() throws Exception {
    int threadAmount = 10;
    List<Runnable> runnables = new ArrayList<>();
    Evictor evictor = mock(Evictor.class);
    when(
        evictor.freeSpaceWithView(any(Long.class), any(BlockStoreLocation.class),
            any(BlockMetadataManagerView.class), any(Mode.class)))
        .thenAnswer((InvocationOnMock invocation) -> {
          CommonUtils.sleepMs(20);
          return new EvictionPlan(new ArrayList<>(), new ArrayList<>());
        }
      );
    Field field = mBlockStore.getClass().getDeclaredField("mEvictor");
    field.setAccessible(true);
    field.set(mBlockStore,
        evictor);
    for (int i = 0; i < threadAmount; i++) {
      runnables.add(() -> {
        try {
          mBlockStore.freeSpace(SESSION_ID1, 0, new BlockStoreLocation("MEM", 0));
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
   * Tests the {@link TieredBlockStore#createBlock(long, long, BlockStoreLocation, long)} method
   * to work without eviction.
   */
  @Test
  public void createBlockMetaWithoutEviction() throws Exception {
    TempBlockMeta tempBlockMeta = mBlockStore.createBlock(SESSION_ID1, TEMP_BLOCK_ID,
        mTestDir1.toBlockStoreLocation(), 1);
    assertEquals(1, tempBlockMeta.getBlockSize());
    assertEquals(mTestDir1, tempBlockMeta.getParentDir());
  }

  /**
   * Tests the {@link TieredBlockStore#createBlock(long, long, BlockStoreLocation, long)} method
   * to work with eviction.
   */
  @Test
  public void createBlockMetaWithEviction() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    TempBlockMeta tempBlockMeta = mBlockStore.createBlock(SESSION_ID1, TEMP_BLOCK_ID,
        mTestDir1.toBlockStoreLocation(), mTestDir1.getCapacityBytes());
    // Expect BLOCK_ID1 evicted from mTestDir1
    assertFalse(mTestDir1.hasBlockMeta(BLOCK_ID1));
    assertFalse(FileUtils.exists(BlockMeta.commitPath(mTestDir1, BLOCK_ID1)));
    assertEquals(mTestDir1.getCapacityBytes(), tempBlockMeta.getBlockSize());
    assertEquals(mTestDir1, tempBlockMeta.getParentDir());
  }

  /**
   * Tests that when creating a block, if the space of the target location is currently taken by
   * another block being locked, this creation operation will fail until the lock released.
   */
  @Test
  public void createBlockMetaWithBlockLocked() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);

    // session1 locks a block first
    long lockId = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID1);

    // Expect an exception because no eviction plan is feasible
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(
        ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE.getMessage(mTestDir1.getCapacityBytes(),
            mTestDir1.toBlockStoreLocation().tierAlias()));
    mBlockStore.createBlock(SESSION_ID1, TEMP_BLOCK_ID, mTestDir1.toBlockStoreLocation(),
        mTestDir1.getCapacityBytes());

    // Expect createBlockMeta to succeed after unlocking this block.
    mBlockStore.unlockBlock(lockId);
    mBlockStore.createBlock(SESSION_ID1, TEMP_BLOCK_ID, mTestDir1.toBlockStoreLocation(),
        mTestDir1.getCapacityBytes());
    assertEquals(0, mTestDir1.getAvailableBytes());
  }

  /**
   * Tests that when moving a block from src location to dst, if the space of the dst location is
   * currently taken by another block being locked, this move operation will fail until the lock
   * released.
   */
  @Test
  public void moveBlockMetaWithBlockLocked() throws Exception {
    // Setup the src dir containing the block to move
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    // Setup the dst dir whose space is totally taken by another block
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID2, mTestDir2.getCapacityBytes(), mTestDir2,
        mMetaManager, mEvictor);

    // session1 locks block2 first
    long lockId = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID2);

    // Expect an exception because no eviction plan is feasible
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE.getMessage(
        BLOCK_SIZE, mTestDir2.toBlockStoreLocation().tierAlias()));
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1, mTestDir2.toBlockStoreLocation());

    // Expect createBlockMeta to succeed after unlocking this block.
    mBlockStore.unlockBlock(lockId);
    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1, mTestDir2.toBlockStoreLocation());

    assertEquals(mTestDir1.getCapacityBytes(), mTestDir1.getAvailableBytes());
    assertEquals(mTestDir2.getCapacityBytes() - BLOCK_SIZE, mTestDir2.getAvailableBytes());
  }

  /**
   * Tests that when free the space of a location, if the space of the target location is currently
   * taken by another block being locked, this freeSpace operation will fail until the lock
   * released.
   */
  @Test
  public void freeSpaceWithBlockLocked() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);

    // session1 locks a block first
    long lockId = mBlockStore.lockBlock(SESSION_ID1, BLOCK_ID1);

    // Expect an empty eviction plan is feasible
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE.getMessage(
        mTestDir1.getCapacityBytes(), mTestDir1.toBlockStoreLocation().tierAlias()));
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes(),
        mTestDir1.toBlockStoreLocation());

    // Expect freeSpace to succeed after unlock this block.
    mBlockStore.unlockBlock(lockId);
    mBlockStore.freeSpace(SESSION_ID1, mTestDir1.getCapacityBytes(),
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
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID);
    mBlockStore.abortBlock(SESSION_ID1, TEMP_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown when trying to move a block which does not exist.
   */
  @Test
  public void moveNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.moveBlock(SESSION_ID1, BLOCK_ID1, mTestDir1.toBlockStoreLocation());
  }

  /**
   * Tests that an exception is thrown when trying to move a temporary block.
   */
  @Test
  public void moveTempBlock() throws Exception {
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK.getMessage(TEMP_BLOCK_ID));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.moveBlock(SESSION_ID1, TEMP_BLOCK_ID, mTestDir2.toBlockStoreLocation());
  }

  /**
   * Tests that an exception is thrown when trying to cache a block which already exists in a
   * different directory.
   */
  @Test
  public void cacheSameBlockInDifferentDirs() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(BLOCK_ID1,
        FIRST_TIER_ALIAS));
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir2, mMetaManager,
        mEvictor);
  }

  /**
   * Tests that an exception is thrown when trying to cache a block which already exists in a
   * different tier.
   */
  @Test
  public void cacheSameBlockInDifferentTiers() throws Exception {
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(BLOCK_ID1,
        FIRST_TIER_ALIAS));
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir3, mMetaManager,
        mEvictor);
  }

  /**
   * Tests that an exception is thrown when trying to commit a block twice.
   */
  @Test
  public void commitBlockTwice() throws Exception {
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED.getMessage(TEMP_BLOCK_ID));

    TieredBlockStoreTestUtils.createTempBlock(SESSION_ID1, TEMP_BLOCK_ID, BLOCK_SIZE, mTestDir1);
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID);
    mBlockStore.commitBlock(SESSION_ID1, TEMP_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown when trying to commit a block which does not exist.
   */
  @Test
  public void commitNonExistingBlock() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(BLOCK_ID1));

    mBlockStore.commitBlock(SESSION_ID1, BLOCK_ID1);
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
    mBlockStore.commitBlock(SESSION_ID2, TEMP_BLOCK_ID);
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
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID1, BLOCK_SIZE, mTestDir1, mMetaManager,
        mEvictor);
    TieredBlockStoreTestUtils.cache(SESSION_ID1, BLOCK_ID2, BLOCK_SIZE, mTestDir2, mMetaManager,
        mEvictor);
    BlockStoreMeta oldMeta = mBlockStore.getBlockStoreMeta();
    FileUtils.deletePathRecursively(mTestDir2.getDirPath());
    assertTrue("check storage should fail if one of the directory is not accessible",
        mBlockStore.checkStorage());
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
