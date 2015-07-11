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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class BlockLockManagerTest {
  private static final long TEST_USER_ID = 2;
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
    mLockManager = new BlockLockManager(mockMetaManager);
  }

  @Test
  public void lockNonExistingBlockTest() throws Exception {
    long nonExistingBlockId = TEST_BLOCK_ID + 1;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to lockBlock: no blockId " + nonExistingBlockId + " found");
    mLockManager.lockBlock(TEST_USER_ID, nonExistingBlockId, BlockLockType.READ);
  }

  @Test
  public void lockBlockTest() throws Exception {
    // Read-lock on can both get through
    long lockId1 = mLockManager.lockBlock(TEST_USER_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(TEST_USER_ID, TEST_BLOCK_ID, BlockLockType.READ);
    Assert.assertNotEquals(lockId1, lockId2);
  }

  @Test
  public void unlockNonExistingLockTest() throws Exception {
    long badBockId = 1;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to unlockBlock: lockId " + badBockId + " has no lock record");
    // Unlock a non-existing lockId, expect to see IOException
    mLockManager.unlockBlock(badBockId);
  }

  @Test
  public void validateLockIdWithNoRecordTest() throws Exception {
    long badLockId = 1;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to validateLock: lockId " + badLockId + " has no lock record");
    // Validate a non-existing lockId, expect to see IOException
    mLockManager.validateLock(TEST_USER_ID, TEST_BLOCK_ID, badLockId);
  }

  @Test
  public void validateLockIdWithWrongUserIdTest() throws Exception {
    long lockId = mLockManager.lockBlock(TEST_USER_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long wrongUserId = TEST_USER_ID + 1;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to validateLock: lockId " + lockId + " is owned by userId "
        + TEST_USER_ID + ", not " + wrongUserId);
    // Validate an existing lockId with wrong userId, expect to see IOException
    mLockManager.validateLock(wrongUserId, TEST_BLOCK_ID, lockId);
  }

  @Test
  public void validateLockIdWithWrongBlockIdTest() throws Exception {
    long lockId = mLockManager.lockBlock(TEST_USER_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long wrongBlockId = TEST_BLOCK_ID + 1;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to validateLock: lockId " + lockId + " is for blockId "
        + TEST_BLOCK_ID + ", not " + wrongBlockId);
    // Validate an existing lockId with wrong blockId, expect to see IOException
    mLockManager.validateLock(TEST_USER_ID, wrongBlockId, lockId);
  }

  @Test
  public void cleanupUserTest() throws Exception {
    long userId1 = TEST_USER_ID;
    long userId2 = TEST_USER_ID + 1;
    long lockId1 = mLockManager.lockBlock(userId1, TEST_BLOCK_ID, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(userId2, TEST_BLOCK_ID, BlockLockType.READ);
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to validateLock: lockId " + lockId2 + " has no lock record");
    mLockManager.cleanupUser(userId2);
    // Expect validating userId1 to get through
    mLockManager.validateLock(userId1, TEST_BLOCK_ID, lockId1);
    // Because userId2 has been cleaned up, expect validating userId2 to throw IOException
    mLockManager.validateLock(userId2, TEST_BLOCK_ID, lockId2);
  }
}
