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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.security.authorization.Mode;

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
    assertEquals(Mode.defaults().applyFileUMask().toShort(), inode1.getMode());
  }

  /**
   * Tests the {@link Inode#lockRead()} and {@link Inode#unlockRead()} methods.
   */
  @Test
  public void lockRead() {
    InodeFile inode1 = createInodeFile(1);
    assertFalse(inode1.isReadLocked());
    assertFalse(inode1.isWriteLocked());
    inode1.lockRead();
    assertTrue(inode1.isReadLocked());
    assertFalse(inode1.isWriteLocked());
    inode1.unlockRead();
    assertFalse(inode1.isReadLocked());
    assertFalse(inode1.isWriteLocked());
  }

  /**
   * Tests the {@link Inode#lockReadAndCheckParent(InodeView)} method.
   */
  @Test
  public void lockReadAndCheckParent() throws Exception {
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setParentId(dir1.getId());
    inode1.lockReadAndCheckParent(dir1);
    assertTrue(inode1.isReadLocked());
    inode1.unlockRead();
  }

  /**
   * Tests the {@link Inode#lockReadAndCheckParent(InodeView)} method fails when the parent is
   * not consistent.
   */
  @Test
  public void lockReadAndCheckParentInvalid() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setParentId(dir1.getId() - 1);
    inode1.lockReadAndCheckParent(dir1);
  }

  /**
   * Tests the {@link Inode#lockReadAndCheckNameAndParent(InodeView, String)} method.
   */
  @Test
  public void lockReadAndCheckNameAndParent() throws Exception {
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId());
    inode1.lockReadAndCheckNameAndParent(dir1, name);
    assertTrue(inode1.isReadLocked());
    inode1.unlockRead();
  }

  /**
   * Tests the {@link Inode#lockReadAndCheckNameAndParent(InodeView, String)} method fails when the
   * parent and name are not consistent.
   */
  @Test
  public void lockReadAndCheckNameAndParentInvalid() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId() - 1);
    inode1.lockReadAndCheckNameAndParent(dir1, "invalid");
  }

  /**
   * Tests the {@link Inode#lockReadAndCheckNameAndParent(InodeView, String)} method fails when the
   * name is not consistent.
   */
  @Test
  public void lockReadAndCheckNameAndParentInvalidName() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId());
    inode1.lockReadAndCheckNameAndParent(dir1, "invalid");
  }

  /**
   * Tests the {@link Inode#lockReadAndCheckNameAndParent(InodeView, String)} method fails when the
   * parent is not consistent.
   */
  @Test
  public void lockReadAndCheckNameAndParentInvalidParent() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId() - 1);
    inode1.lockReadAndCheckNameAndParent(dir1, name);
  }

  /**
   * Tests the {@link Inode#lockWrite()} and {@link Inode#unlockWrite()} methods.
   */
  @Test
  public void lockWrite() {
    InodeFile inode1 = createInodeFile(1);
    inode1.lockWrite();
    assertFalse(inode1.isReadLocked());
    assertTrue(inode1.isWriteLocked());
    inode1.unlockWrite();
    assertFalse(inode1.isReadLocked());
    assertFalse(inode1.isWriteLocked());
  }

  /**
   * Tests the {@link Inode#lockWriteAndCheckParent(InodeView)} method.
   */
  @Test
  public void lockWriteAndCheckParent() throws Exception {
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setParentId(dir1.getId());
    inode1.lockWriteAndCheckParent(dir1);
    assertTrue(inode1.isWriteLocked());
    inode1.unlockWrite();
  }

  /**
   * Tests the {@link Inode#lockWriteAndCheckParent(InodeView)} method fails when the parent is
   * not consistent.
   */
  @Test
  public void lockWriteAndCheckParentInvalid() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setParentId(dir1.getId() - 1);
    inode1.lockWriteAndCheckParent(dir1);
  }

  /**
   * Tests the {@link Inode#lockWriteAndCheckNameAndParent(InodeView, String)} method.
   */
  @Test
  public void lockWriteAndCheckNameAndParent() throws Exception {
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId());
    inode1.lockWriteAndCheckNameAndParent(dir1, name);
    assertTrue(inode1.isWriteLocked());
    inode1.unlockWrite();
  }

  /**
   * Tests the {@link Inode#lockWriteAndCheckNameAndParent(InodeView, String)} method fails when the
   * parent and name are not consistent.
   */
  @Test
  public void lockWriteAndCheckNameAndParentInvalid() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId() - 1);
    inode1.lockWriteAndCheckNameAndParent(dir1, "invalid");
  }

  /**
   * Tests the {@link Inode#lockWriteAndCheckNameAndParent(InodeView, String)} method fails when the
   * name is not consistent.
   */
  @Test
  public void lockWriteAndCheckNameAndParentInvalidName() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId());
    inode1.lockWriteAndCheckNameAndParent(dir1, "invalid");
  }

  /**
   * Tests the {@link Inode#lockWriteAndCheckNameAndParent(InodeView, String)} method fails when the
   * parent is not consistent.
   */
  @Test
  public void lockWriteAndCheckNameAndParentInvalidParent() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    String name = "file";
    InodeFile inode1 = createInodeFile(1);
    InodeDirectory dir1 = createInodeDirectory();
    inode1.setName(name);
    inode1.setParentId(dir1.getId() - 1);
    inode1.lockWriteAndCheckNameAndParent(dir1, name);
  }
}
