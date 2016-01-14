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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidWorkerStateException;

/**
 * Unit tests for {@link BlockLockManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMetadataManager.class)
public class BlockLockManagerTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;

  private BlockLockManager mLockManager;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the test fails
   */
  @Before
  public void before() throws Exception {
    BlockMetadataManager mockMetaManager = PowerMockito.mock(BlockMetadataManager.class);
    PowerMockito.when(mockMetaManager.hasBlockMeta(TEST_BLOCK_ID)).thenReturn(true);
    mLockManager = new BlockLockManager();
  }

  /**
   * Tests the {@link BlockLockManager#lockBlock(long, long, BlockLockType)} method.
   */
  @Test
  public void lockBlockTest() {
    // Read-lock on can both get through
    long lockId1 = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    Assert.assertNotEquals(lockId1, lockId2);
  }

  /**
   * Tests that an exception is thrown when trying to unlock a block via
   * {@link BlockLockManager#unlockBlock(long)} which is not locked.
   *
   * @throws Exception if unlocking the block fails
   */
  @Test
  public void unlockNonExistingLockTest() throws Exception {
    long badLockId = 1;
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(badLockId));
    // Unlock a non-existing lockId, expect to see IOException
    mLockManager.unlockBlock(badLockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} which is not locked.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void validateLockIdWithNoRecordTest() throws Exception {
    long badLockId = 1;
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(badLockId));
    // Validate a non-existing lockId, expect to see IOException
    mLockManager.validateLock(TEST_SESSION_ID, TEST_BLOCK_ID, badLockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} with an incorrect session ID.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void validateLockIdWithWrongSessionIdTest() throws Exception {
    long lockId = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long wrongSessionId = TEST_SESSION_ID + 1;
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_SESSION.getMessage(lockId,
        TEST_SESSION_ID, wrongSessionId));
    // Validate an existing lockId with wrong sessionId, expect to see IOException
    mLockManager.validateLock(wrongSessionId, TEST_BLOCK_ID, lockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} with an incorrect block ID.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void validateLockIdWithWrongBlockIdTest() throws Exception {
    long lockId = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long wrongBlockId = TEST_BLOCK_ID + 1;
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_BLOCK.getMessage(lockId,
        TEST_BLOCK_ID, wrongBlockId));
    // Validate an existing lockId with wrong blockId, expect to see IOException
    mLockManager.validateLock(TEST_SESSION_ID, wrongBlockId, lockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} after the session was cleaned up.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void cleanupSessionTest() throws Exception {
    long sessionId1 = TEST_SESSION_ID;
    long sessionId2 = TEST_SESSION_ID + 1;
    long lockId1 = mLockManager.lockBlock(sessionId1, TEST_BLOCK_ID, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(sessionId2, TEST_BLOCK_ID, BlockLockType.READ);
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(lockId2));
    mLockManager.cleanupSession(sessionId2);
    // Expect validating sessionId1 to get through
    mLockManager.validateLock(sessionId1, TEST_BLOCK_ID, lockId1);
    // Because sessionId2 has been cleaned up, expect validating sessionId2 to throw IOException
    mLockManager.validateLock(sessionId2, TEST_BLOCK_ID, lockId2);
  }
}
