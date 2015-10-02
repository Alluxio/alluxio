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
import org.mockito.Mockito;

import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidWorkerStateException;

public class BlockLockManagerTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;

  private BlockLockManager mLockManager;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    BlockMetadataManager mockMetaManager = Mockito.mock(BlockMetadataManager.class);
    Mockito.when(mockMetaManager.hasBlockMeta(TEST_BLOCK_ID)).thenReturn(true);
    mLockManager = new BlockLockManager();
  }

  @Test
  public void lockBlockTest() throws Exception {
    // Read-lock on can both get through
    long lockId1 = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    Assert.assertNotEquals(lockId1, lockId2);
  }

  @Test
  public void unlockNonExistingLockTest() throws Exception {
    long badLockId = 1;
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(badLockId));
    // Unlock a non-existing lockId, expect to see IOException
    mLockManager.unlockBlock(badLockId);
  }

  @Test
  public void validateLockIdWithNoRecordTest() throws Exception {
    long badLockId = 1;
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(badLockId));
    // Validate a non-existing lockId, expect to see IOException
    mLockManager.validateLock(TEST_SESSION_ID, TEST_BLOCK_ID, badLockId);
  }

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
