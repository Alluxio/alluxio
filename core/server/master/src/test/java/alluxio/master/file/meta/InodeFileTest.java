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

package alluxio.master.file.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.exception.BlockInfoException;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link InodeFile}.
 */
public final class InodeFileTest extends AbstractInodeTest {
  private static final long LENGTH = 100;

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  /**
   * Tests the {@link InodeFile#equals(Object)} method.
   */
  @Test
  public void equals() {
    InodeFile inode1 = createInodeFile(1);
    // self equal
    assertEquals(inode1, inode1);
    InodeFile inode2 = createInodeFile(1);
    // equal with same id
    assertEquals(inode1, inode2);
    InodeFile inode3 = createInodeFile(3);
    assertFalse(inode1.equals(inode3));
  }

  /**
   * Tests the {@link InodeFile#getId()} method.
   */
  @Test
  public void getId() {
    InodeFile inode1 = createInodeFile(1);
    assertEquals(createInodeFileId(1), inode1.getId());
  }

  /**
   * Tests the {@link InodeFile#setLength(long)} method.
   */
  @Test
  public void setLength() {
    InodeFile inodeFile = createInodeFile(1);
    inodeFile.setLength(LENGTH);
    assertEquals(LENGTH, inodeFile.getLength());
  }

  /**
   * Tests the {@link InodeFile#getBlockSizeBytes()} method.
   */
  @Test
  public void getBlockSizeBytes() {
    InodeFile inode1 = createInodeFile(1);
    assertEquals(Constants.KB, inode1.getBlockSizeBytes());
  }

  /**
   * Tests the {@link InodeFile#getBlockIdByIndex(int)} method.
   */
  @Test
  public void getBlockIdByIndex() throws Exception {
    InodeFile inodeFile = createInodeFile(1);
    List<Long> blockIds = new ArrayList<>();
    final int NUM_BLOCKS = 3;
    for (int i = 0; i < NUM_BLOCKS; i++) {
      blockIds.add(inodeFile.getNewBlockId());
    }
    for (int i = 0; i < NUM_BLOCKS; i++) {
      assertEquals(blockIds.get(i), (Long) inodeFile.getBlockIdByIndex(i));
    }
    try {
      inodeFile.getBlockIdByIndex(-1);
      Assert.fail();
    } catch (BlockInfoException e) {
      assertEquals(String.format("blockIndex -1 is out of range. File blocks: %d",
          NUM_BLOCKS), e.getMessage());
    }
    try {
      inodeFile.getBlockIdByIndex(NUM_BLOCKS);
      Assert.fail();
    } catch (BlockInfoException e) {
      assertEquals(String.format("blockIndex %d is out of range. File blocks: %d",
          NUM_BLOCKS, NUM_BLOCKS), e.getMessage());
    }
  }

  /**
   * Tests the {@link InodeFile#setCompleted(boolean)} method.
   */
  @Test
  public void setCompleted() {
    InodeFile inode1 = createInodeFile(1);
    assertFalse(inode1.isCompleted());

    inode1.setCompleted(true);
    assertTrue(inode1.isCompleted());
  }

  /**
   * Tests the {@link InodeFile#getMode()} method.
   */
  @Test
  public void permissionStatus() {
    InodeFile inode1 = createInodeFile(1);
    assertEquals(TEST_OWNER, inode1.getOwner());
    assertEquals(TEST_GROUP, inode1.getGroup());
    assertEquals(ModeUtils.applyFileUMask(Mode.defaults()).toShort(), inode1.getMode());
  }
}
